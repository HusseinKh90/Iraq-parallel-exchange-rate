#!/usr/bin/env python3
"""Build the two small files the published site needs on top of the CSVs.

    <out>/data/dataset_stats.json   the numbers behind the "Dataset at a glance" line
    <out>/social/card.png           the 1200 x 630 image shown when the page is shared

Both are computed from the published CSVs on every workflow run, so they follow the
data and never go stale. Neither file is committed to the repository.

    python scripts/build_site_assets.py --data-dir data --out-dir _site
"""
import argparse
import csv
import json
import statistics
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PAGE_URL = "husseinkh90.github.io/Iraq-parallel-exchange-rate"
MINUS = "\u2212"


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def num(value):
    return float(value) if value not in ("", None) else None


# --------------------------------------------------------------------------- stats
def compute_stats(data_dir):
    daily = read_csv(Path(data_dir) / "daily_fx.csv")
    dates = [date.fromisoformat(r["date"]) for r in daily]
    have = set(dates)

    # Trading days are Sunday to Thursday; Friday (4) and Saturday (5) have no quotes.
    expected = missing = 0
    day = dates[0]
    while day <= dates[-1]:
        if day.weekday() not in (4, 5):
            expected += 1
            if day not in have:
                missing += 1
        day += timedelta(days=1)

    city = read_csv(Path(data_dir) / "consensus_city_day.csv")
    gaps = [float(r["spread_pct"]) for r in city
            if r["spread_pct"] != "" and int(r["n_sources"]) > 1]
    validation = read_csv(Path(data_dir) / "validate_fx.csv")

    return {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_through": dates[-1].isoformat(),
        "first_date": dates[0].isoformat(),
        "observations": len(daily),
        "trading_days_expected": expected,
        "trading_days_missing": missing,
        "coverage_pct": round(100 * (expected - missing) / expected, 1),
        "markets": len({r["city"] for r in city}),
        "source_gap_median_pct": round(statistics.median(gaps), 2) if gaps else None,
        "source_gap_days": len(gaps),
        "validation_days": len(validation),
        "validation_from": validation[0]["date"] if validation else None,
        "validation_to": validation[-1]["date"] if validation else None,
    }


# --------------------------------------------------------------------------- card
FONT_DIRS = [
    "/usr/share/fonts/truetype/dejavu/", "/usr/share/fonts/dejavu/", "/usr/share/fonts/TTF/",
    "/Library/Fonts/", "/System/Library/Fonts/Supplemental/", "C:/Windows/Fonts/",
]


def load_font(names, size):
    from PIL import ImageFont
    for folder in FONT_DIRS:
        for name in names:
            path = Path(folder) / name
            if path.exists():
                return ImageFont.truetype(str(path), size)
    return ImageFont.load_default(size)


def wrap(draw, text, font, width):
    lines, line = [], ""
    for word in text.split():
        trial = (line + " " + word).strip()
        if draw.textlength(trial, font=font) <= width or not line:
            line = trial
        else:
            lines.append(line)
            line = word
    if line:
        lines.append(line)
    return lines


def make_card(data_dir, stats, out_path):
    from PIL import Image, ImageDraw

    daily = read_csv(Path(data_dir) / "daily_fx.csv")
    series = [(date.fromisoformat(r["date"]), num(r["baghdad_close"])) for r in daily]
    series = [(d, v) for d, v in series if v is not None]
    last_date, last = series[-1]
    month_ago = next(v for d, v in reversed(series) if d <= last_date - timedelta(days=30))
    change = (last - month_ago) / month_ago * 100
    year = [(d, v) for d, v in series if d > last_date - timedelta(days=365)]
    in_progress = daily[-1]["basra_close"] == "" or daily[-1]["erbil_close"] == ""

    S, W, H = 2, 1200, 630          # draw at 2x, then downsample for smooth edges
    BG, INK, MUTED, TEAL, RULE = (248, 250, 249), (26, 37, 40), (86, 100, 104), (11, 111, 108), (211, 220, 219)
    img = Image.new("RGB", (W * S, H * S), BG)
    draw = ImageDraw.Draw(img)
    sans = lambda size: load_font(["DejaVuSans.ttf", "Arial.ttf", "LiberationSans-Regular.ttf"], size * S)
    bold = lambda size: load_font(["DejaVuSans-Bold.ttf", "Arial Bold.ttf", "LiberationSans-Bold.ttf"], size * S)
    serif = lambda size: load_font(["DejaVuSerif-Bold.ttf", "Georgia Bold.ttf", "LiberationSerif-Bold.ttf"], size * S)
    px = lambda v: int(v * S)
    left, right = 64, W - 64

    # brand line, letter-spaced
    x = left
    for ch in "IRAQ PARALLEL EXCHANGE RATE":
        draw.text((px(x), px(40)), ch, font=bold(20), fill=TEAL)
        x += draw.textlength(ch, font=bold(20)) / S + 2.2

    # headline
    head = f"The US dollar exchange rate is {last:,.0f} dinars in Baghdad\u2019s parallel market"
    size = 52                       # shrink until the headline fits on two lines
    while size > 40 and len(wrap(draw, head, serif(size), px(right - left))) > 2:
        size -= 2
    y = 78
    for line in wrap(draw, head, serif(size), px(right - left)):
        draw.text((px(left), px(y)), line, font=serif(size), fill=INK)
        y += round(size * 1.23)
    sub = f"{last * 100:,.0f} dinars per $100  \u00b7  {'+' if change >= 0 else MINUS}{abs(change):.1f}% in 30 days  \u00b7  {last_date.day} {last_date.strftime('%b %Y')}"
    if in_progress:
        sub += "  (day in progress)"
    draw.text((px(left), px(y + 6)), sub, font=sans(26), fill=MUTED)

    # 12-month line
    top, bottom = 272, 462
    lo, hi = min(v for _, v in year), max(v for _, v in year)
    pad = (hi - lo) * 0.12 or 1
    lo, hi = lo - pad, hi + pad
    t0, t1 = year[0][0].toordinal(), year[-1][0].toordinal()
    X = lambda d: left + (d.toordinal() - t0) / (t1 - t0) * (right - left)
    Y = lambda v: bottom - (v - lo) / (hi - lo) * (bottom - top)
    for frac in (0, 0.5, 1):
        gy = top + frac * (bottom - top)
        draw.line([(px(left), px(gy)), (px(right), px(gy))], fill=RULE, width=px(1))
    pts = [(px(X(d)), px(Y(v))) for d, v in year]
    draw.line(pts, fill=TEAL, width=px(5), joint="curve")
    ex, ey = pts[-1]
    draw.ellipse([ex - px(8), ey - px(8), ex + px(8), ey + px(8)], fill=TEAL, outline=BG, width=px(3))
    draw.text((px(left), px(bottom + 8)), "Baghdad, daily close, last 12 months", font=sans(17), fill=MUTED)
    rng = f"low {min(v for _, v in year):,.0f}  \u00b7  high {max(v for _, v in year):,.0f}"
    draw.text((px(right) - draw.textlength(rng, font=sans(17)), px(bottom + 8)), rng, font=sans(17), fill=MUTED)

    # dataset at a glance
    draw.line([(px(left), px(506)), (px(right), px(506))], fill=RULE, width=px(1))
    items = [
        (f"{stats['observations']:,}", "daily observations"),
        (f"{stats['coverage_pct']:.1f}%", "of trading days covered"),
        (f"{stats['markets']}", "markets in the open data"),
        (f"{stats['source_gap_median_pct']:.2f}%" if stats["source_gap_median_pct"] is not None else "n/a", "median gap between sources"),
    ]
    col = (right - left) / 4
    for i, (value, label) in enumerate(items):
        cx = left + i * col
        draw.text((px(cx), px(516)), value, font=bold(34), fill=INK)
        draw.text((px(cx), px(558)), label, font=sans(17), fill=MUTED)

    draw.text((px(left), px(596)), "Open data \u00b7 updated daily \u00b7 CC BY 4.0", font=sans(17), fill=MUTED)
    url_w = draw.textlength(PAGE_URL, font=sans(17))
    draw.text((px(right) - url_w, px(596)), PAGE_URL, font=sans(17), fill=TEAL)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.resize((W, H), Image.LANCZOS).save(out_path, optimize=True)


# --------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--out-dir", default="_site")
    ap.add_argument("--no-card", action="store_true", help="write the stats file only")
    args = ap.parse_args()

    stats = compute_stats(args.data_dir)
    out = Path(args.out_dir)
    (out / "data").mkdir(parents=True, exist_ok=True)
    (out / "data" / "dataset_stats.json").write_text(
        json.dumps(stats, indent=2) + "\n", encoding="utf-8")
    print(f"stats: {stats['observations']:,} observations, {stats['coverage_pct']}% of trading days, "
          f"{stats['markets']} markets, median source gap {stats['source_gap_median_pct']}%")

    if not args.no_card:
        make_card(args.data_dir, stats, out / "social" / "card.png")
        print(f"card: {out / 'social' / 'card.png'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
