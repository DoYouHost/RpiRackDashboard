"""Display rendering utilities for multi-node dashboard"""

import os
import time
import threading
from collections import deque
from PIL import Image, ImageDraw, ImageFont
from typing import Callable, Deque, Dict, List, Optional, Any, Tuple
import numpy as np

# ── Font loading ──────────────────────────────────────────────────────────────
_FONT_DIR     = os.path.join(os.path.dirname(__file__), "fonts")
_MONO_REGULAR = os.path.join(_FONT_DIR, "DejaVuSansMono.ttf")
_MONO_BOLD    = os.path.join(_FONT_DIR, "DejaVuSansMono-Bold.ttf")

def _load(path: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(path, size)

FONT_NODE_LABEL  = _load(_MONO_BOLD,    11)
FONT_HEADER_STAT = _load(_MONO_REGULAR, 10)
FONT_BIG_VALUE   = _load(_MONO_BOLD,    18)
FONT_HIST_LABEL  = _load(_MONO_REGULAR,  9)
FONT_PAGE_TITLE  = _load(_MONO_BOLD,    12)

# ── Overview-page exclusive fonts ─────────────────────────────────────────────
# 3-node layout (row ~80px) — 3-column, node-bar 46px, col_w 76px
# 36pt: "100"=65px + 2gap + "%"=8px = 75px ≤ 76px ✓
FONT_OVERVIEW_VALUE  = _load(_MONO_BOLD,    36)   # big metric numbers
FONT_OVERVIEW_UNIT   = _load(_MONO_BOLD,    12)   # '%' and '°' after numbers
FONT_OVERVIEW_LABEL  = _load(_MONO_REGULAR, 10)   # 'CPU'/'RAM'/'TMP' labels
FONT_OVERVIEW_NODEID = _load(_MONO_BOLD,    24)   # 'N1'/'N2'/'N3'
FONT_DETAIL_STAT     = _load(_MONO_BOLD,    28)   # secondary stats on detail page
FONT_STAT_GRID       = _load(_MONO_BOLD,    22)   # 2-row grid stats on detail page

# 1- and 2-node layout — 3-column, node-bar 30px, col_w 82px
# 40pt: "100"=72px + 2gap + "%"=8px = 82px ≤ 82px ✓  (all 3 metrics same size)
FONT_OV_WIDE_VALUE  = _load(_MONO_BOLD,    40)
FONT_OV_WIDE_UNIT   = _load(_MONO_BOLD,    12)
FONT_OV_WIDE_LABEL  = _load(_MONO_REGULAR, 10)
FONT_OV_WIDE_NODEID = _load(_MONO_BOLD,    24)   # 24pt "N1"=29px fits 30px bar

# ── getbbox memoizer — font metric lookups are expensive; cache by (font, text) ─
_BB: Dict[Any, Any] = {}

def _bb(font: ImageFont.FreeTypeFont, text: str) -> tuple:
    key = (id(font), text)
    v = _BB.get(key)
    if v is None:
        v = font.getbbox(text)
        _BB[key] = v
    return v

# Warm cache for all fixed layout strings known at import time
for _s in (
    "CPU", "RAM", "TMP", "%", "\u00b0C", "100", "99", "--",
    "N1", "N2", "N3", "NODE 1", "NODE 2", "NODE 3",
    "FREQ", "DISK", "UPTIME", "LOAD", "NET DN", "NET UP",
    "overview", "detail",
):
    for _f in (
        FONT_OVERVIEW_VALUE, FONT_OVERVIEW_UNIT, FONT_OVERVIEW_LABEL,
        FONT_OVERVIEW_NODEID, FONT_HIST_LABEL, FONT_STAT_GRID,
        FONT_BIG_VALUE, FONT_HEADER_STAT, FONT_NODE_LABEL,
        FONT_OV_WIDE_VALUE, FONT_OV_WIDE_UNIT, FONT_OV_WIDE_LABEL, FONT_OV_WIDE_NODEID,
    ):
        _bb(_f, _s)


# ── Per-node accent palette ───────────────────────────────────────────────────
NODE_COLORS: Dict[str, str] = {
    "node1": "#00FF88",
    "node2": "#00CCFF",
    "node3": "#FF44AA",
}
_DEFAULT_COLOR = "#FFFFFF"

# ── Internal histogram ring buffers ───────────────────────────────────────────
_hist_cpu: Dict[str, Deque[float]] = {}
_hist_ram: Dict[str, Deque[float]] = {}
_HIST_LEN = 320

def _ensure_hist(node_id: str) -> None:
    if node_id not in _hist_cpu:
        _hist_cpu[node_id] = deque([0.0] * _HIST_LEN, maxlen=_HIST_LEN)
        _hist_ram[node_id] = deque([0.0] * _HIST_LEN, maxlen=_HIST_LEN)

def _push(buf: Dict[str, Deque[float]], node_id: str, value: Optional[float]) -> None:
    buf[node_id].append(value if value is not None else 0.0)

def _health_color(value: float, low: float = 60, high: float = 85) -> str:
    if value < low:
        return "#00FF88"
    elif value < high:
        return "#FFCC00"
    return "#FF4444"

def _health_color_temp(value: float) -> str:
    return _health_color(value, low=50, high=65)


# ── Page system ───────────────────────────────────────────────────────────────
# A page is a callable:  fn(draw, width, height, data) -> None
# `data` is whatever dict the display loop passes in (node metrics, etc.)
PageFn = Callable[[ImageDraw.ImageDraw, int, int, Dict[str, Any]], None]

class PageManager:
    """Holds an ordered list of pages with a static thumbnail switcher.

    Calling next()/prev() triggers the switcher: all page thumbnails are
    rendered once into a cached image and displayed for DWELL_SEC seconds,
    then the new page commits.  No per-frame re-rendering during the dwell —
    the cached frame is blitted directly, keeping Pi CPU usage low.
    Thread-safe throughout.
    """

    # How long the switcher stays visible before committing (seconds).
    # Keep generous — rendering on real hardware is slow.
    DWELL_SEC = 2.5

    def __init__(self) -> None:
        self._pages: List[Tuple[str, PageFn]] = []
        self._index: int = 0
        self._lock  = threading.Lock()

        # Transition state (guarded by _lock)
        self._switching:      bool            = False
        self._target_index:   int             = 0
        self._switch_started: float           = 0.0
        # Cached switcher frame — built once per switch, blitted on every
        # subsequent render call until the dwell expires.
        self._switcher_cache: Optional[Image.Image] = None
        # Event to wake the display loop immediately when page nav is triggered
        self._wake_event: threading.Event = threading.Event()
        # Track time of last navigation for inactivity timeout
        self._last_nav_time: float = time.monotonic()

    # ── Registration ─────────────────────────────────────────────────────────

    def register(self, name: str, fn: PageFn) -> None:
        self._pages.append((name, fn))

    def unregister(self, name: str) -> bool:
        """Remove a page by name. Adjusts current index safely. Returns True if found."""
        with self._lock:
            idx = next((i for i, (n, _) in enumerate(self._pages) if n == name), None)
            if idx is None:
                return False
            self._pages.pop(idx)
            if not self._pages:
                self._index = 0
                self._target_index = 0
                self._switching = False
                self._switcher_cache = None
                return True
            # Adjust current index
            if idx <= self._index:
                self._index = max(0, self._index - 1)
            self._index = min(self._index, len(self._pages) - 1)
            # Adjust switching target
            if self._switching:
                if idx == self._target_index:
                    self._switching = False
                    self._switcher_cache = None
                elif idx < self._target_index:
                    self._target_index -= 1
                self._target_index = min(self._target_index, len(self._pages) - 1)
            return True

    def get_names(self) -> list:
        """Return list of currently registered page names."""
        with self._lock:
            return [name for name, _ in self._pages]

    # ── Navigation (thread-safe, called from button/keyboard) ────────────────

    def next(self) -> None:
        with self._lock:
            if not self._pages:
                return
            self._last_nav_time = time.monotonic()
            base = self._target_index if self._switching else self._index
            self._start_switch((base + 1) % len(self._pages))

    def prev(self) -> None:
        with self._lock:
            if not self._pages:
                return
            self._last_nav_time = time.monotonic()
            base = self._target_index if self._switching else self._index
            self._start_switch((base - 1) % len(self._pages))

    def _start_switch(self, target: int) -> None:
        """Must be called with _lock held."""
        self._switching       = True
        self._target_index    = target
        self._switch_started  = time.monotonic()
        self._switcher_cache  = None   # invalidate cache; rebuilt on next render
        self._wake_event.set()  # wake the display loop immediately

    def go_to_overview(self) -> None:
        """Force navigation to the overview page."""
        with self._lock:
            overview_idx = next((i for i, (name, _) in enumerate(self._pages)
                               if name == "overview"), None)
            if overview_idx is not None and overview_idx != self._index:
                self._last_nav_time = time.monotonic()
                self._start_switch(overview_idx)

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def is_switching(self) -> bool:
        with self._lock:
            return self._switching

    @property
    def wake_event(self) -> threading.Event:
        return self._wake_event

    @property
    def last_nav_time(self) -> float:
        with self._lock:
            return self._last_nav_time

    @property
    def current_name(self) -> str:
        with self._lock:
            if not self._pages:
                return ""
            return self._pages[self._index][0]

    @property
    def page_count(self) -> int:
        with self._lock:
            return len(self._pages)

    # ── Rendering ────────────────────────────────────────────────────────────

    def render(self, draw: ImageDraw.ImageDraw, width: int, height: int,
               data: Dict[str, Any]) -> None:
        with self._lock:
            if not self._pages:
                return
            switching      = self._switching
            index          = self._index
            target         = self._target_index
            elapsed        = time.monotonic() - self._switch_started
            pages_snapshot = list(self._pages)

            # Commit once dwell time is over
            if switching and elapsed >= self.DWELL_SEC:
                self._index          = target
                self._switching      = False
                self._switcher_cache = None
                switching            = False
                index                = target

            # Build the switcher frame once and cache it
            if switching and self._switcher_cache is None:
                self._switcher_cache = _build_switcher_image(
                    width, height, data, pages_snapshot, target,
                )

            cache = self._switcher_cache

        if switching and cache is not None:
            # Blit cached switcher — no per-frame page rendering
            draw._image.paste(cache, (0, 0))  # type: ignore[attr-defined]
        else:
            _name, fn = pages_snapshot[index]
            fn(draw, width, height, data)


# ── Switcher overlay ──────────────────────────────────────────────────────────

def _render_page_thumbnail(fn: PageFn, width: int, height: int,
                           data: Dict[str, Any]) -> Image.Image:
    """Render a page into a small off-screen buffer and return it."""
    buf  = Image.new("RGB", (width, height), (0, 0, 0))
    bdraw = ImageDraw.Draw(buf)
    fn(bdraw, width, height, data)
    return buf


def _build_switcher_image(
    width:  int,
    height: int,
    data:   Dict[str, Any],
    pages:  List[Tuple[str, PageFn]],
    target: int,
) -> Image.Image:
    """Render the switcher frame once and return it as an Image.

    Called once per switch event; the result is cached by PageManager
    and pasted on every subsequent render call during the dwell period.
    No animated elements — static frame only to minimise Pi CPU load.
    """
    n   = len(pages)
    img = Image.new("RGB", (width, height), (0, 0, 0))
    d   = ImageDraw.Draw(img)

    # Thumbnail dimensions
    THUMB_W  = 72    # inactive  (320px / ~4 pages leaves room for gaps)
    THUMB_H  = 54
    ACTIVE_W = 100   # highlighted page is larger
    ACTIVE_H = 75
    GAP      = 8
    LABEL_H  = 13

    total_w = (n - 1) * (THUMB_W + GAP) + ACTIVE_W
    start_x = (width - total_w) // 2
    ty_dim  = (height - THUMB_H  - LABEL_H) // 2
    ty_big  = (height - ACTIVE_H - LABEL_H) // 2

    cx = start_x
    for i, (name, fn) in enumerate(pages):
        is_active = (i == target)
        tw = ACTIVE_W if is_active else THUMB_W
        th = ACTIVE_H if is_active else THUMB_H
        ty = ty_big   if is_active else ty_dim

        # Render the page at full resolution then downscale — done once only
        thumb_full = _render_page_thumbnail(fn, width, height, data)
        thumb      = thumb_full.resize((tw, th), Image.Resampling.LANCZOS)
        img.paste(thumb, (cx, ty))

        # Border: 2px bright white for active, 1px dim for inactive
        border_color = "#FFFFFF" if is_active else "#444444"
        border_px    = 2 if is_active else 1
        for b in range(border_px):
            d.rectangle(
                [cx - b - 1, ty - b - 1, cx + tw + b, ty + th + b],
                outline=border_color,
            )

        # Page name below thumbnail
        bb      = _bb(FONT_HIST_LABEL, name)
        lw      = bb[2] - bb[0]
        label_x = cx + (tw - lw) // 2
        label_y = ty + th + 3
        d.text((label_x, label_y), name,
               font=FONT_HIST_LABEL,
               fill="#FFFFFF" if is_active else "#666666")

        cx += tw + GAP

    return img


# ── Helpers ───────────────────────────────────────────────────────────────────

def _format_elapsed(seconds: float) -> str:
    """Format seconds into a compact string: '5s', '3m', '2h', '1d'."""
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h"
    return f"{int(seconds // 86400)}d"


def make_node_detail_page(node_id: str) -> "PageFn":
    """Return a PageFn that always renders the detail view for the given node_id."""
    def _fn(draw: ImageDraw.ImageDraw, width: int, height: int,
            data: Dict[str, Any]) -> None:
        _page_detail(draw, width, height, {**data, "detail_node": node_id})
    return _fn


# ── Page: overview (3-node histogram dashboard) ───────────────────────────────

def _page_overview(draw: ImageDraw.ImageDraw, width: int, height: int,
                   data: Dict[str, Any]) -> None:
    nodes: List[str] = data.get("node_ids", [])
    if not nodes:
        return

    node_height = height // len(nodes)

    for idx, node_id in enumerate(nodes):
        row_y     = idx * node_height
        node_info = data.get(node_id, {})

        if idx > 0:
            draw.line([(20, row_y), (299, row_y)], fill="#2A2A2A", width=1)

        draw_node_section_overview(
            draw,
            row_y=row_y,
            node_height=node_height,
            node_id=node_id,
            cpu_usage=node_info.get("cpu_usage"),
            ram_usage=node_info.get("ram_usage"),
            cpu_temp=node_info.get("cpu_temp"),
        )


# ── Arc gauge helper ──────────────────────────────────────────────────────────
def _draw_arc_gauge(
    draw: ImageDraw.ImageDraw,
    cx: int, cy: int,
    radius: int,
    value: float,
    color: str,
    label: str,
    val_str: str,
    val_font: ImageFont.FreeTypeFont,
    lbl_font: ImageFont.FreeTypeFont,
    thickness: int = 5,
) -> None:
    """Draw a horseshoe arc gauge (200°–340°) centred at (cx, cy).

    PIL angles: 0=3 o'clock, clockwise.
    200°–340° is the lower arc (opens upward like a U).
    cy is the arc centre — arc appears BELOW cy.
    Label goes above cy, value inside the U below cy.
    """
    bbox = [cx - radius, cy - radius, cx + radius, cy + radius]

    draw.arc(bbox, start=200, end=340, fill="#2A2A2A", width=thickness)

    v = max(0.0, min(1.0, value))
    if v > 0:
        draw.arc(bbox, start=200, end=200 + 140 * v, fill=color, width=thickness)

    # Label above cy
    bb_l = _bb(lbl_font, label)
    lx = cx - (bb_l[2] - bb_l[0]) // 2 - bb_l[0]
    ly = cy - (bb_l[3] - bb_l[1]) - 3
    draw.text((lx, ly), label, font=lbl_font, fill="#AAAAAA")

    # Value below cy, inside the U
    bb_v = _bb(val_font, val_str)
    vx = cx - (bb_v[2] - bb_v[0]) // 2 - bb_v[0]
    vy = cy + 4 - bb_v[1]
    draw.text((vx, vy), val_str, font=val_font, fill=color)


# ── Page: single-node detail ──────────────────────────────────────────────────
# Shows one node at a time, full-screen histogram + extra stats.
# `data["detail_node"]` controls which node is shown.

def _page_detail(draw: ImageDraw.ImageDraw, width: int, height: int,
                 data: Dict[str, Any]) -> None:
    node_id   = data.get("detail_node", "node1")
    node_info = data.get(node_id, {})
    accent    = NODE_COLORS.get(node_id, _DEFAULT_COLOR)
    node_num  = node_id.replace("node", "")

    cpu_usage   = node_info.get("cpu_usage")
    ram_usage   = node_info.get("ram_usage")
    cpu_temp    = node_info.get("cpu_temp")
    cpu_freq    = node_info.get("cpu_freq")
    uptime      = node_info.get("uptime")
    disk_usage  = node_info.get("disk_usage")
    net_tx_rate = node_info.get("net_tx_rate")
    net_rx_rate = node_info.get("net_rx_rate")
    load_avg_5  = node_info.get("load_avg_5")

    # ── Offline status ────────────────────────────────────────────────────────
    last_seen_ts = data.get("node_last_seen", {}).get(node_id)
    is_offline   = False
    offline_label = ""
    if last_seen_ts is not None:
        elapsed = time.time() - last_seen_ts
        if elapsed > 30:
            is_offline    = True
            offline_label = _format_elapsed(elapsed)

    # ── Title bar ─────────────────────────────────────────────────────────────
    # Slimmed to 24px (was 30): 24pt font visual height ~18px, 3px padding each side
    TITLE_H   = 24
    bar_fill  = "#3A0000" if is_offline else accent
    txt_fill  = "#FFFFFF" if is_offline else "#000000"
    draw.rectangle([20, 0, 299, TITLE_H - 1], fill=bar_fill)
    title = f"NODE {node_num}"
    bb_t  = _bb(FONT_OVERVIEW_NODEID, title)
    tx    = 20 + (280 - (bb_t[2] - bb_t[0])) // 2 - bb_t[0]
    ty    = (TITLE_H - (bb_t[3] - bb_t[1])) // 2 - bb_t[1]
    draw.text((tx, ty), title, font=FONT_OVERVIEW_NODEID, fill=txt_fill)
    if is_offline:
        tag    = f"OFFLINE {offline_label}"
        bb_tag = _bb(FONT_HIST_LABEL, tag)
        tag_x  = 299 - (bb_tag[2] - bb_tag[0]) - 4
        tag_y  = (TITLE_H - (bb_tag[3] - bb_tag[1])) // 2 - bb_tag[1]
        draw.text((tag_x, tag_y), tag, font=FONT_HIST_LABEL, fill="#FF6644")

    # ── Big metric columns (CPU / RAM / TEMP) ─────────────────────────────────
    # 3 cols × 92px + 2 gaps × 2px = 280px (x=20..299)
    DT_COL_W      = 92
    DT_COL_STARTS = (20, 114, 208)
    DT_COL_LABELS = ("CPU", "RAM", "TMP")
    DT_COL_UNITS  = ("%", "%", "\u00b0C")
    DT_LABEL_Y    = TITLE_H + 4    # 28 — tighter spacing after slimmer title
    DT_VALUE_Y    = TITLE_H + 14   # 38 — 40pt glyph visible y≈46..76
    DT_BAR_Y0     = DT_VALUE_Y + 42  # 80 — 4px gap below glyph bottom
    DT_BAR_Y1     = DT_BAR_Y0 + 5   # 85 — 6px tall bar
    DT_UNIT_GAP   = 2

    raw_vals = (cpu_usage, ram_usage, cpu_temp)
    for col_idx, (col_x, label, unit, raw_val) in enumerate(
        zip(DT_COL_STARTS, DT_COL_LABELS, DT_COL_UNITS, raw_vals)
    ):
        # Column label
        bb_lab = _bb(FONT_OVERVIEW_LABEL, label)
        lab_w  = bb_lab[2] - bb_lab[0]
        lab_x  = col_x + (DT_COL_W - lab_w) // 2
        draw.text((lab_x, DT_LABEL_Y), label, font=FONT_OVERVIEW_LABEL, fill="#AAAAAA")

        # Value — right-aligned, max digits 3/3/2
        max_str   = "99" if col_idx == 2 else "100"
        bb_max    = _bb(FONT_OVERVIEW_VALUE, max_str)
        max_num_w = bb_max[2] - bb_max[0]
        bb_unit   = _bb(FONT_OVERVIEW_UNIT, unit)
        unit_w    = bb_unit[2] - bb_unit[0]
        col_right = col_x + DT_COL_W - 1
        unit_x    = col_right - unit_w
        val_right = unit_x - DT_UNIT_GAP

        if raw_val is not None:
            val_str   = str(int(raw_val))
            val_color = _health_color_temp(raw_val) if col_idx == 2 else _health_color(raw_val)
        else:
            val_str   = "--"
            val_color = "#444444"

        bb_num = _bb(FONT_OVERVIEW_VALUE, val_str)
        num_w  = bb_num[2] - bb_num[0]
        val_x  = val_right - max_num_w + (max_num_w - num_w)

        draw.text((val_x, DT_VALUE_Y), val_str, font=FONT_OVERVIEW_VALUE, fill=val_color)

        if raw_val is not None:
            draw.text((unit_x, DT_VALUE_Y), unit, font=FONT_OVERVIEW_UNIT, fill="#888888")

        # Progress bar — 6px tall, identical style to overview page
        draw.rectangle(
            [col_x, DT_BAR_Y0, col_x + DT_COL_W - 1, DT_BAR_Y1],
            fill="#1A1A1A",
        )
        if raw_val is not None:
            frac  = max(0.0, min(1.0, raw_val / (80.0 if col_idx == 2 else 100.0)))
            bar_w = max(1, int(frac * DT_COL_W))
            draw.rectangle(
                [col_x, DT_BAR_Y0, col_x + bar_w - 1, DT_BAR_Y1],
                fill=val_color,
            )

    # ── Separator ─────────────────────────────────────────────────────────────
    SEP_Y = DT_BAR_Y1 + 6   # 91 — anchored just below progress bars
    draw.line([(20, SEP_Y), (299, SEP_Y)], fill="#2A2A2A", width=1)

    # ── 3-column vertical dividers ─────────────────────────────────────────────
    draw.line([(113, SEP_Y), (113, 239)], fill="#2A2A2A", width=1)
    draw.line([(206, SEP_Y), (206, 239)], fill="#2A2A2A", width=1)

    # ── Mid-row divider — full width across all 3 columns ─────────────────────
    GRID_MID_Y = 166
    draw.line([(20, GRID_MID_Y), (299, GRID_MID_Y)], fill="#2A2A2A", width=1)

    # ── Layout constants ───────────────────────────────────────────────────────
    # Col 2 (x=114..205, w=92) is a merged cell spanning both rows for NET↓/↑
    NET_COL_X, NET_COL_W = 114, 92
    # Two 73px rows (y=93..165 and y=167..239), content centred in each half
    SSTAT_R2_LBL_Y = 185   # row 2 label top Y
    SSTAT_R2_VAL_Y = 201   # row 2 value reference Y (before -bb[1] correction)

    # ── Value strings ──────────────────────────────────────────────────────────
    uptime_str = _format_elapsed(int(time.time() - uptime)) if uptime is not None else "--"

    if disk_usage is not None:
        disk_str   = f"{disk_usage:.0f}%"
        disk_color = _health_color(disk_usage)
    else:
        disk_str   = "--"
        disk_color = "#444444"

    def _fmt_net(v: Optional[float]) -> str:
        if v is None:
            return "--"
        if v < 1_000:
            return f"{v:.0f}B"
        if v < 1_000_000:
            return f"{v / 1_000:.1f}K"
        return f"{v / 1_000_000:.1f}M"

    load_str = f"{load_avg_5:.2f}" if load_avg_5 is not None else "--"

    if cpu_freq is not None:
        freq_str = f"{cpu_freq/1000:.1f}GHz" if cpu_freq >= 1000 else f"{cpu_freq:.0f}MHz"
    else:
        freq_str = "--"

    # ── Col 1 row 2 + Col 3 rows 1 & 2 + Col 1 row 1 (FREQ) ──────────────────
    # Col 2 bottom row only: NET DN + NET UP stacked in y=167..239 (72px) ──────
    # Each item: label ~12px + value ~20px = 32px each, gap 4px → 68px fits nicely.
    for label, val_str, val_color, lbl_y, val_y in (
        ("NET DN", _fmt_net(net_rx_rate), "#00FF88", 172, 184),
        ("NET UP", _fmt_net(net_tx_rate), "#00CCFF", 206, 218),
    ):
        bb_sl = _bb(FONT_OVERVIEW_LABEL, label)
        sl_x  = NET_COL_X + (NET_COL_W - (bb_sl[2] - bb_sl[0])) // 2
        draw.text((sl_x, lbl_y), label, font=FONT_OVERVIEW_LABEL, fill="#AAAAAA")

        bb_sv = _bb(FONT_STAT_GRID, val_str)
        sv_x  = NET_COL_X + (NET_COL_W - (bb_sv[2] - bb_sv[0])) // 2 - bb_sv[0]
        sv_y  = val_y - bb_sv[1]
        draw.text((sv_x, sv_y), val_str, font=FONT_STAT_GRID, fill=val_color)

    # Row 1 cell: y=93..165 (73px).
    # Content: label(12) + gap(3) + radius(28) + gap(4) + value(26) = 73px → cy=136
    freq_pct = (cpu_freq / 2400.0) if cpu_freq is not None else 0.0
    _draw_arc_gauge(
        draw, cx=66, cy=136, radius=28,
        value=freq_pct, color="#CCCCCC",
        label="FREQ", val_str=freq_str,
        val_font=FONT_STAT_GRID, lbl_font=FONT_OVERVIEW_LABEL,
    )

    disk_pct = (disk_usage / 100.0) if disk_usage is not None else 0.0
    _draw_arc_gauge(
        draw, cx=252, cy=136, radius=28,
        value=disk_pct, color=disk_color,
        label="DISK", val_str=disk_str,
        val_font=FONT_STAT_GRID, lbl_font=FONT_OVERVIEW_LABEL,
    )

    # ── Col 1 row 2 + Col 3 row 2: text stats ─────────────────────────────────
    for (col_x, col_w), (label, val_str, val_color), lbl_y, val_y in (
        ((20,  93), ("UPTIME", uptime_str, "#CCCCCC"), SSTAT_R2_LBL_Y, SSTAT_R2_VAL_Y),
        ((206, 94), ("LOAD",   load_str,   "#CCCCCC"), SSTAT_R2_LBL_Y, SSTAT_R2_VAL_Y),
    ):
        bb_sl = _bb(FONT_OVERVIEW_LABEL, label)
        sl_x  = col_x + (col_w - (bb_sl[2] - bb_sl[0])) // 2
        draw.text((sl_x, lbl_y), label, font=FONT_OVERVIEW_LABEL, fill="#AAAAAA")

        bb_sv = _bb(FONT_STAT_GRID, val_str)
        sv_x  = col_x + (col_w - (bb_sv[2] - bb_sv[0])) // 2 - bb_sv[0]
        sv_y  = val_y - bb_sv[1]
        draw.text((sv_x, sv_y), val_str, font=FONT_STAT_GRID, fill=val_color)


# ── Overview page layout constants ───────────────────────────────────────────
# ── Layout constants ─────────────────────────────────────────────────────────
_OV_LEFT_BAR_X0 = 20
_OV_UNIT_GAP    = 2

# 3-node: node-bar x=20..65 (46px), 3 cols × 76px with 2px gaps
# 36pt "100"=65px + 2 + "%"=8px = 75px ≤ 76px ✓
_OV_NODEID_X0   = 28
_OV_NODEID_ZONE = 38
_OV_COL_STARTS  = (68, 146, 224)
_OV_COL_W       = 76

# 1/2-node wide: node-bar x=20..49 (30px), 3 cols × 82px with 2px gaps
# 40pt "100"=72px + 2 + "%"=8px = 82px ≤ 82px ✓  (all 3 metrics same size)
_OV_WIDE_BAR_X1  = 49
_OV_WIDE_COL_W   = 82
_OV_WIDE_COL_A   = 50
_OV_WIDE_COL_B   = 134   # 50 + 82 + 2
_OV_WIDE_COL_C   = 218   # 50 + 82 + 2 + 82 + 2


def _draw_value_and_unit(
    draw: ImageDraw.ImageDraw,
    col_x: int, col_w: int,
    val_y: int,
    val_str: str, unit: str,
    max_str: str,
    val_color: str,
    f_val: ImageFont.FreeTypeFont,
    f_unit: ImageFont.FreeTypeFont,
) -> None:
    """Right-align value+unit within col. Value is right-aligned to the reserved
    max-width slot; unit sits flush against the column's right edge."""
    bb_max  = _bb(f_val, max_str)
    max_w   = bb_max[2] - bb_max[0]
    bb_unit = _bb(f_unit, unit)
    unit_w  = bb_unit[2] - bb_unit[0]
    col_r   = col_x + col_w - 1
    unit_x  = col_r - unit_w
    # Value right-aligns within the max-width slot ending at unit_x - gap
    slot_r  = unit_x - _OV_UNIT_GAP
    bb_num  = _bb(f_val, val_str)
    num_w   = bb_num[2] - bb_num[0]
    val_x    = max(col_x, slot_r - max_w + (max_w - num_w))
    draw.text((val_x, val_y), val_str, font=f_val, fill=val_color)
    if val_str != "--":
        # Bottom-align unit with the value glyph
        bb_val_ref = _bb(f_val, max_str)
        unit_y = val_y + (bb_val_ref[3] - bb_unit[3])
        draw.text((unit_x, unit_y), unit, font=f_unit, fill="#888888")


def _draw_metric_col(
    draw: ImageDraw.ImageDraw,
    col_x: int, col_w: int,
    content_y0: int, val_y: int,
    bar_y0: int, bar_y1: int,
    label: str, unit: str,
    raw_val: Optional[float],
    f_val: ImageFont.FreeTypeFont,
    f_unit: ImageFont.FreeTypeFont,
    f_label: ImageFont.FreeTypeFont,
    is_temp: bool = False,
) -> None:
    """Draw label + big value+unit + progress bar for one metric."""
    val_color = (
        _health_color_temp(raw_val) if (is_temp and raw_val is not None)
        else _health_color(raw_val) if raw_val is not None
        else "#444444"
    )
    val_str = str(int(raw_val)) if raw_val is not None else "--"
    max_str = "99" if is_temp else "100"

    # Label centred horizontally
    bb_lab = _bb(f_label, label)
    lab_x  = col_x + (col_w - (bb_lab[2] - bb_lab[0])) // 2
    draw.text((lab_x, content_y0), label, font=f_label, fill="#AAAAAA")

    _draw_value_and_unit(
        draw, col_x, col_w, val_y,
        val_str, unit, max_str, val_color, f_val, f_unit,
    )

    # Progress bar
    draw.rectangle([col_x, bar_y0, col_x + col_w - 1, bar_y1], fill="#1A1A1A")
    if raw_val is not None:
        frac  = max(0.0, min(1.0, raw_val / (80.0 if is_temp else 100.0)))
        bar_w = max(1, int(frac * col_w))
        draw.rectangle([col_x, bar_y0, col_x + bar_w - 1, bar_y1], fill=val_color)


def draw_node_section_overview(
    draw: ImageDraw.ImageDraw,
    row_y: int,
    node_id: str,
    cpu_usage: Optional[float],
    ram_usage: Optional[float],
    cpu_temp: Optional[float],
    node_height: int = 80,
) -> None:
    """Draw one node row for the overview page.

    ≤ 2 nodes (row ≥ 105px): 2-column layout — CPU | RAM at 60pt, TEMP as
                               a compact subtitle below the RAM value.
      3 nodes  (row  ~80px): 3-column layout — CPU | RAM | TMP at 36pt.
    """
    accent     = NODE_COLORS.get(node_id, _DEFAULT_COLOR)
    node_label = f"N{node_id.replace('node', '')}"

    bar_h  = max(4, node_height // 16)
    bar_y0 = row_y + node_height - bar_h - 1
    bar_y1 = bar_y0 + bar_h - 1

    if node_height >= 105:
        # ── Wide 3-column layout (1 or 2 nodes) ──────────────────────────────
        # node-bar 30px, 3 cols × 82px — all metrics same 40pt size
        f_val   = FONT_OV_WIDE_VALUE   # 40pt
        f_unit  = FONT_OV_WIDE_UNIT    # 12pt
        f_label = FONT_OV_WIDE_LABEL   # 10pt
        f_nid   = FONT_OV_WIDE_NODEID  # 24pt

        # Node-bar (30px)
        bb_nid    = _bb(f_nid, node_label)
        bar_rect_w = _OV_WIDE_BAR_X1 - _OV_LEFT_BAR_X0 + 1
        nid_x = _OV_LEFT_BAR_X0 + (bar_rect_w - (bb_nid[2] - bb_nid[0])) // 2 - bb_nid[0]
        nid_y = row_y + (node_height - (bb_nid[3] - bb_nid[1])) // 2 - bb_nid[1]
        draw.rectangle([_OV_LEFT_BAR_X0, row_y, _OV_WIDE_BAR_X1, row_y + node_height - 1], fill=accent)
        draw.text((nid_x, nid_y), node_label, font=f_nid, fill="#000000")

        # Shared vertical layout
        bb_val_ref = _bb(f_val, "100")
        val_h      = bb_val_ref[3] - bb_val_ref[1]
        bb_lab_ref = _bb(f_label, "CPU")
        lab_h      = bb_lab_ref[3] - bb_lab_ref[1]
        content_h  = lab_h + 2 + val_h
        content_y0 = row_y + (node_height - bar_h - 2 - content_h) // 2
        val_y      = content_y0 + lab_h + 2

        for col_idx, (col_x, label, unit) in enumerate(
            zip((_OV_WIDE_COL_A, _OV_WIDE_COL_B, _OV_WIDE_COL_C),
                ("CPU", "RAM", "TMP"), ("%", "%", "\u00b0C"))
        ):
            raw_val = (cpu_usage, ram_usage, cpu_temp)[col_idx]
            _draw_metric_col(
                draw,
                col_x=col_x, col_w=_OV_WIDE_COL_W,
                content_y0=content_y0, val_y=val_y,
                bar_y0=bar_y0, bar_y1=bar_y1,
                label=label, unit=unit, raw_val=raw_val,
                f_val=f_val, f_unit=f_unit, f_label=f_label,
                is_temp=(col_idx == 2),
            )

    else:
        # ── Standard 3-column layout (3 nodes, row ~80px) ────────────────────
        f_val   = FONT_OVERVIEW_VALUE   # 36pt
        f_unit  = FONT_OVERVIEW_UNIT    # 12pt
        f_label = FONT_OVERVIEW_LABEL   # 10pt
        f_nid   = FONT_OVERVIEW_NODEID  # 24pt

        # Node-bar (46px: x=20..65)
        bb_nid = _bb(f_nid, node_label)
        rect_w = _OV_NODEID_X0 + _OV_NODEID_ZONE - _OV_LEFT_BAR_X0
        nid_x  = _OV_LEFT_BAR_X0 + (rect_w - (bb_nid[2] - bb_nid[0])) // 2 - bb_nid[0]
        nid_y  = row_y + (node_height - (bb_nid[3] - bb_nid[1])) // 2 - bb_nid[1]
        draw.rectangle(
            [_OV_LEFT_BAR_X0, row_y, _OV_NODEID_X0 + _OV_NODEID_ZONE - 1, row_y + node_height - 1],
            fill=accent,
        )
        draw.text((nid_x, nid_y), node_label, font=f_nid, fill="#000000")

        # Shared vertical layout
        bb_val_ref = _bb(f_val, "100")
        val_h      = bb_val_ref[3] - bb_val_ref[1]
        bb_lab_ref = _bb(f_label, "CPU")
        lab_h      = bb_lab_ref[3] - bb_lab_ref[1]
        content_h  = lab_h + 2 + val_h
        content_y0 = row_y + (node_height - bar_h - 2 - content_h) // 2
        val_y      = content_y0 + lab_h + 2

        for col_idx, (col_x, label, unit) in enumerate(
            zip(_OV_COL_STARTS, ("CPU", "RAM", "TMP"), ("%", "%", "\u00b0C"))
        ):
            raw_val = (cpu_usage, ram_usage, cpu_temp)[col_idx]
            _draw_metric_col(
                draw,
                col_x=col_x, col_w=_OV_COL_W,
                content_y0=content_y0, val_y=val_y,
                bar_y0=bar_y0, bar_y1=bar_y1,
                label=label, unit=unit, raw_val=raw_val,
                f_val=f_val, f_unit=f_unit, f_label=f_label,
                is_temp=(col_idx == 2),
            )


# ── NumPy histogram renderer ─────────────────────────────────────────────────

# Pre-built colour arrays (RGB tuples as uint8 numpy rows) for the 3 bands
_HIST_FILL = np.array([
    [0,  51, 25],   # < 50  fill  #003319
    [61, 46,  0],   # < 75  fill  #3D2E00
    [61,  0,  0],   # >= 75 fill  #3D0000
], dtype=np.uint8)

_HIST_TOP = np.array([
    [0,  255, 136],  # < 50  top  #00FF88
    [255, 204,  0],  # < 75  top  #FFCC00
    [255,  68, 68],  # >= 75 top  #FF4444
], dtype=np.uint8)


def _draw_histogram_np(
    draw: ImageDraw.ImageDraw,
    cx: int,
    hist_y0: int,
    hist_y1: int,
    hist_w: int,
    history: Deque[float],
) -> None:
    """Render CPU histogram into the draw image using NumPy array writes."""
    hist_h = hist_y1 - hist_y0
    cols   = min(len(history), hist_w)

    # Build a (hist_h, hist_w, 3) array; start fully dark
    arr = np.empty((hist_h, hist_w, 3), dtype=np.uint8)
    arr[:] = (8, 8, 8)  # #080808 background

    if cols == 0:
        draw._image.paste(Image.fromarray(arr, "RGB"), (cx, hist_y0))  # type: ignore[attr-defined]
        return

    vals = np.fromiter(history, dtype=np.float32, count=len(history))
    vals = vals[-cols:]                               # rightmost `cols` samples
    bar_heights = np.maximum(1, (vals * (hist_h / 100.0)).astype(np.int32))

    # Colour band index per column: 0 (<50), 1 (<75), 2 (>=75)
    band = np.where(vals < 50, 0, np.where(vals < 75, 1, 2))
    fill_colors = _HIST_FILL[band]   # shape (cols, 3)
    top_colors  = _HIST_TOP[band]    # shape (cols, 3)

    offset = hist_w - cols
    for i in range(cols):
        bh      = int(bar_heights[i])
        bar_top = hist_h - bh
        col     = offset + i
        if bh > 1:
            arr[bar_top + 1:hist_h, col] = fill_colors[i]
        arr[bar_top, col] = top_colors[i]

    draw._image.paste(Image.fromarray(arr, "RGB"), (cx, hist_y0))  # type: ignore[attr-defined]


def push_node_metrics(node_id: str, cpu_usage: Optional[float], ram_usage: Optional[float]) -> None:
    """Push CPU and RAM samples into the histogram ring buffers.

    Call this from the display loop on every tick regardless of active page,
    so the detail-page histogram stays populated even when overview is shown.
    """
    _ensure_hist(node_id)
    _push(_hist_cpu, node_id, cpu_usage)
    _push(_hist_ram, node_id, ram_usage)


# ── Build the default PageManager ─────────────────────────────────────────────

page_manager = PageManager()
page_manager.register("overview", _page_overview)
# Per-node detail pages are registered dynamically from main.py
