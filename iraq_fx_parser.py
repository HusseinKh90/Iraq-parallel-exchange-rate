# -*- coding: utf-8 -*-
"""Iraqi parallel-market USD/IQD exchange rate — scraper, parser and dataset builder.

Reads publicly posted exchange-rate quotations from four Iraqi Telegram
channels and builds a daily open/close series for the Baghdad, Basra and Erbil
wholesale markets.

    python iraq_fx.py scrape        download new posts   -> data/raw/*.jsonl
    python iraq_fx.py parse         build the dataset    -> data/daily_fx.csv
                                                        -> data/validate_fx.csv
    python iraq_fx.py check         health check on the published series
    python iraq_fx.py audit --channel dollar_price    posts that parsed to nothing

The pipeline, in order:

    Telegram  ->  data/raw/<channel>.jsonl      one JSON object per post
              ->  data/readings/<channel>.csv   every reading KEPT, with the
                                                message it came from. Retail
                                                and the de-scoped cities are
                                                matched during extraction but
                                                discarded before this file, so
                                                it is not everything found.
              ->  data/daily/<channel>.csv      open/close per market per day,
                                                one file per channel
              ->  data/consensus_city_day.csv   the channels compared: one row
                                                per market per day, with each
                                                channel's figure and the spread
              ->  data/validate_fx.csv          2018-01-01 .. 2020-11-30,
                                                closes only — the stretch used
                                                to check the method against the
                                                Central Bank of Iraq
              ->  data/daily_fx.csv             2020-12-01 onward — the dataset

Also written, off to the side of that chain:

    data/consensus_daily.csv        the same consensus as one row per day,
                                    every market side by side
    data/unmatched/<channel>.jsonl  posts that parsed to nothing; what the
                                    `audit` command reads
    data/parse_report.json          per-channel post, reading and match counts
    data/scrape_report.json         written by `scrape`, not by `parse`

Baghdad, Basra and Erbil are published as three separate series. There is no
aggregate across them.

Each rate is an average across the Telegram channels that quoted that market
that day; `channels_used` on each row names them. Baghdad additionally has
several wholesale sub-markets, and `baghdad_market` names which one the day's
Baghdad figure describes.

Sections below follow that order. Jump to the one you need:

    TEXT CLEANING            Arabic/Kurdish folding, reading a price token
    MARKETS AND CITIES       what a reading is; the city dictionary
    EXTRACTION               pairing a city name with the nearest price
    CHANNEL FORMATS          one section per channel — edit here when a
                             channel changes how it posts
    POST -> READINGS         running the right format over one post
    SCRAPING                 fetching posts from t.me
    DAILY TABLE              first/last per day, then averaging channels
    PUBLISH                  writing daily_fx.csv
    COMMAND LINE             argument parsing and the commands above
"""

import argparse
import csv
import json
import random
import re
import shutil
import statistics
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Pattern, Tuple


# ==========================================================================
# TEXT CLEANING  —  Arabic/Kurdish folding and price tokens
# ==========================================================================

# ---------------------------------------------------------------------------
# Plausible IQD-per-USD band. Inherited from the original single-channel
# parser: the street rate stayed inside this range across the whole corpus,
# so anything outside is contamination or a parse error, not a rate.
# ---------------------------------------------------------------------------

# Calibrated against all 76,521 readings extracted from the five channels'
# full history (Mar 2017 - Jul 2026): the 0.1st percentile is 1195 and the
# 99.9th is 1673, and the parallel rate's real extremes over that period are
# roughly 1180 (2017-19) to 1750 (the Feb 2023 peak). The band keeps headroom
# on both sides while still rejecting contamination.
#
# Every reading that fell outside this band was checked by hand and every one
# was an error: trade quantities ("100,000" dollars offered), a Brent price
# ("109 $"), a Fahrenheit temperature ("114 ف"), and three of the channels'
# own typos ("بغداد 105.950" for 150.950).
#
# Revisit if the market moves outside these bounds -- values beyond them are
# dropped, not clamped.
MIN_PLAUSIBLE_RATE = 1150.0
MAX_PLAUSIBLE_RATE = 1850.0

# All five channels quote IQD per 100 USD. Working in that unit internally
# avoids repeated /100 rounding; convert once at the end.
MIN_PER100 = MIN_PLAUSIBLE_RATE * 100
MAX_PER100 = MAX_PLAUSIBLE_RATE * 100


# ---------------------------------------------------------------------------
# 1. Character folding
# ---------------------------------------------------------------------------

# Invisible characters that Telegram posts are riddled with. @Kukh_alomlat in
# particular prefixes nearly every line with runs of ZWNJ/LRM/RLM, so
# anchoring a pattern to ^ or to a word boundary fails without this.
_INVISIBLE = "".join([
    "​", "‌", "‍", "‎", "‏",   # ZWSP ZWNJ ZWJ LRM RLM
    "‪", "‫", "‬", "‭", "‮",   # bidi embedding/override
    "⁦", "⁧", "⁨", "⁩",             # bidi isolates
    "­",                                            # soft hyphen
    "ـ",                                            # kashida / tatweel
    "﻿",                                            # BOM
])

_INVISIBLE_RE = re.compile(f"[{re.escape(_INVISIBLE)}]")

# Arabic diacritics (fatha, damma, tanween ...). @Kukh_alomlat's header is
# literally "#ًسعر#صرف" with a stray tanween wedged after the hash.
_DIACRITICS_RE = re.compile(r"[ً-ٰٟ]")

# Letter folding. Persian yeh/kaf, alef maksura, the four alef forms, and
# taa marbuta are all folded so that one pattern matches every spelling seen:
#   الحارثية / حارثيه   الكرادة / كراده   البصرة / بصره / البصره
_LETTER_FOLD = str.maketrans({
    "ی": "ي", "ى": "ي", "ئ": "ي",          # Persian yeh, alef maksura, yeh-hamza
    "ک": "ك",                                # Persian kaf
    "أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا",  # hamzated alefs -> bare alef
    "ة": "ه",                                # taa marbuta -> haa
    "ۀ": "ه", "ە": "ه",                      # Kurdish/Persian heh variants
    "ؤ": "و",                                # waw-hamza (سمؤال -> سموال)
})

# Arabic-Indic and extended Arabic-Indic digits -> ASCII. @Kukh_alomlat writes
# commissions as "٣ الف لكل مليون"; leaving these unfolded means a \d pattern
# in Unicode mode matches them unpredictably.
_DIGIT_FOLD = str.maketrans({
    **{chr(0x0660 + i): str(i) for i in range(10)},
    **{chr(0x06f0 + i): str(i) for i in range(10)},
})

# URLs and @handles carry digits (message ids, usernames like @dollar_price) that
# look exactly like prices. Blanked before any number is extracted.
_URL_RE = re.compile(r"https?://\S+|(?<![\w])t\.me/\S+", re.IGNORECASE)
_HANDLE_RE = re.compile(r"@[A-Za-z0-9_]+")

# Telegram web-preview furniture that rides along with the message text.
_VIEWS_RE = re.compile(r"\b[\d.,]+\s*[KM]?\s*views\b.*$", re.IGNORECASE)
_FORWARD_RE = re.compile(r"^\s*Forwarded from .*?$", re.MULTILINE)
_PREMIUM_RE = re.compile(
    r"Please open Telegram to view this post|VIEW IN TELEGRAM", re.IGNORECASE)


def strip_furniture(text: str) -> str:
    """Remove web-preview chrome: forward headers, view counts and
    premium-emoji notices. Does NOT remove URLs (see `normalize`) and does NOT
    remove poll markers -- profiles exclude polls by looking for exactly those
    markers, so stripping them here would make every poll look like an
    ordinary post.
    """
    t = _FORWARD_RE.sub(" ", text)
    t = _PREMIUM_RE.sub(" ", t)
    t = _VIEWS_RE.sub(" ", t)
    return t


def normalize(text: str, *, keep_urls: bool = False) -> str:
    """Fold a raw post into the canonical form every matcher expects.

    Applies NFKC, strips invisibles/diacritics/kashida, folds letters and
    digits, blanks URLs and @handles, drops styling characters (# _ ~ *),
    and collapses runs of whitespace to single spaces while preserving
    newlines (several profiles are line-oriented).
    """
    t = unicodedata.normalize("NFKC", text)
    t = _INVISIBLE_RE.sub("", t)
    t = _DIACRITICS_RE.sub("", t)
    t = t.translate(_LETTER_FOLD)
    t = t.translate(_DIGIT_FOLD)

    if not keep_urls:
        t = _URL_RE.sub(" ", t)
        t = _HANDLE_RE.sub(" ", t)

    # '#', '_', '~' and '*' are pure styling in this corpus. @Kukh_alomlat's
    # early era hides the sell label inside a hashtag ("#بيع_الدولار 123.350"),
    # so these are replaced with a space rather than deleted, which would glue
    # "بيع" to "الدولار".
    t = re.sub(r"[#_~*]+", " ", t)

    # Normalize line structure: collapse horizontal whitespace, keep newlines.
    t = re.sub(r"[ \t ]+", " ", t)
    t = re.sub(r"\n{2,}", "\n", t)
    t = "\n".join(line.strip() for line in t.split("\n"))
    return t.strip()


def flatten(text: str) -> str:
    """Normalized text with newlines collapsed to spaces, for patterns that
    should match across a line break."""
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# 2. Price tokens
#
# Every channel quotes IQD per 100 USD, but the notation varies wildly:
#
#   153.500   period thousands       all channels
#   153,500   comma thousands        all channels
#   151.70    truncated tail         @dollar_price era D  (= 151,700)
#   151       bare 3-digit           @iqborsa late era    (= 151,000)
#
# The lookarounds are load-bearing: without them `\d{6}` matches inside the
# phone number 07710060095 that sits in @Kukh_alomlat's every footer.
# ---------------------------------------------------------------------------

_NOT_DIGIT_BEFORE = r"(?<![\d])"
_NOT_DIGIT_AFTER = r"(?![\d])"

_SEP = r"[.,،]"

# Head is \d{2,3}: a 2-digit head yields <100,000 per 100 USD (<1000 IQD/USD)
# which the plausibility band rejects anyway, but allowing it keeps the
# pattern honest about what appears in the text.
PAT_SEP3 = rf"{_NOT_DIGIT_BEFORE}\d{{2,3}}\s?{_SEP}\s?\d{{3}}{_NOT_DIGIT_AFTER}"
PAT_PLAIN6 = rf"{_NOT_DIGIT_BEFORE}\d{{6}}{_NOT_DIGIT_AFTER}"
PAT_SPACED = rf"{_NOT_DIGIT_BEFORE}\d{{3}} \d{{3}}{_NOT_DIGIT_AFTER}"
PAT_TRUNC2 = rf"{_NOT_DIGIT_BEFORE}\d{{3}}{_SEP}\d{{2}}{_NOT_DIGIT_AFTER}"
PAT_SHORT3 = rf"{_NOT_DIGIT_BEFORE}\d{{3}}{_NOT_DIGIT_AFTER}"

#: Safe default: only unambiguous six-significant-digit forms.
PRICE_DEFAULT = f"(?:{PAT_SEP3}|{PAT_PLAIN6})"
PRICE_WITH_SPACE = f"(?:{PAT_SPACED}|{PAT_SEP3}|{PAT_PLAIN6})"
#: Tolerates a truncated tail: "151.70" (= 151,700) and the malformed
#: "150,00" seen once in @Kukh_alomlat. Still rejects bare 3-digit numbers,
#: which are almost always percentages, counts or foreign-currency figures.
PRICE_TRUNC = f"(?:{PAT_SEP3}|{PAT_PLAIN6}|{PAT_TRUNC2})"
#: Adds bare 3-digit prices ("بغداد 151"). Only safe on very short posts;
#: see the `short_post` guard in the @iqborsa and @dollar_price profiles.
PRICE_LOOSE = f"(?:{PAT_SEP3}|{PAT_PLAIN6}|{PAT_TRUNC2}|{PAT_SHORT3})"

_DIGITS_RE = re.compile(r"\d")


def to_rate(token: str):
    """Convert a matched price token to IQD per 1 USD, or None if implausible.

    Scale is inferred from the significant-digit count rather than from the
    separator, because the separator is unreliable: "151.70" is 151,700 and
    "151" is 151,000, both quoted per 100 USD.

        '153.500' -> 153500 per100 -> 1535.0
        '157000'  -> 157000        -> 1570.0
        '150 300' -> 150300        -> 1503.0
        '151.70'  -> 151700        -> 1517.0
        '151'     -> 151000        -> 1510.0
    """
    digits = "".join(_DIGITS_RE.findall(token))
    n = len(digits)
    if not 3 <= n <= 6:
        return None
    per100 = int(digits) * (10 ** (6 - n))
    if not (MIN_PER100 <= per100 <= MAX_PER100):
        return None
    return per100 / 100.0


# ==========================================================================
# MARKETS AND CITIES  —  what a reading is, and the city dictionary
# ==========================================================================

def round_half_up(x, places: int = 2):
    """Round half away from zero, the way a spreadsheet does.

    Python's built-in round() is round-half-to-even, so round(1500.125, 2)
    gives 1500.12 while Excel's ROUND gives 1500.13. For a dataset published
    as a CSV that people will open in a spreadsheet, that discrepancy is a
    reproducibility bug: a rate recomputed in a spreadsheet came out with a
    different last digit. Every rounded value in the pipeline goes through
    this, so the published files agree with a spreadsheet to the last digit.
    """
    if x is None:
        return None
    return float(Decimal(repr(float(x))).quantize(Decimal((0, (1,), -places)),
                                                  rounding=ROUND_HALF_UP))

# ---------------------------------------------------------------------------
# Market layer. Mixing these is the fastest way to corrupt the series: a
# retail exchange office sells at a markup over the wholesale borsa, so the
# two are different prices for different things, not two readings of one price.
# ---------------------------------------------------------------------------

LAYER_BORSA = "borsa"        # wholesale exchange floor (بورصة)
LAYER_RETAIL = "retail"      # exchange office / صيرفة counter rate
LAYER_OFFICIAL = "official"  # CBI official rate / auction

# ---------------------------------------------------------------------------
# Quote side. Explicit bids are held out of the headline series; everything
# else feeds it. Unmarked quotes are the overwhelming majority across all
# five channels and behave as market/mid prints, so excluding them would
# discard most of the corpus.
# ---------------------------------------------------------------------------

SIDE_OFFER = "offer"      # عرض / ع   — ask
SIDE_BID = "bid"          # طلب / ط / مطلوب — bid
SIDE_SELL = "sell"        # البيع     — office sells USD to you (retail ask)
SIDE_BUY = "buy"          # الشراء    — office buys USD from you (retail bid)
SIDE_TRADED = "traded"    # انباع     — a completed transaction
SIDE_UNKNOWN = "unknown"  # no marker

#: Sides that represent an ask or a print, i.e. the headline series.
ASK_SIDES = frozenset({SIDE_OFFER, SIDE_SELL, SIDE_TRADED, SIDE_UNKNOWN})
#: Sides that represent a bid; reported separately, never averaged in.
BID_SIDES = frozenset({SIDE_BID, SIDE_BUY})


@dataclass
class Reading:
    """One (city, rate) pair extracted from one post."""
    city: str
    rate: float          # IQD per 1 USD
    side: str = SIDE_UNKNOWN
    layer: str = LAYER_BORSA
    matcher: str = ""    # which format matcher produced this, for auditing

    def key(self):
        return (self.city, self.layer, self.side)


@dataclass
class Matcher:
    """One posting format.

    `signature` is a cheap precondition: if it does not match the normalized
    text, `fn` is never called. Matchers are tried in profile order and the
    first one that yields readings wins, so a channel's eras are resolved by
    what the text actually looks like rather than by a message-id cutoff --
    which matters because these channels drift back and forth between formats
    rather than switching cleanly.
    """
    id: str
    fn: Callable[[str], List[Reading]]
    signature: Optional[Pattern] = None
    note: str = ""


@dataclass
class ChannelProfile:
    channel: str
    title: str
    default_layer: str = LAYER_BORSA
    matchers: List[Matcher] = field(default_factory=list)
    #: (reason_code, pattern) — post is dropped whole if the pattern matches.
    excludes: List[tuple] = field(default_factory=list)
    #: Patterns blanked out before matching (foreign currency, gold, ...).
    strips: List[Pattern] = field(default_factory=list)
    #: Weekend days to drop, Python weekday numbers (Fri=4, Sat=5).
    weekend: frozenset = frozenset({4, 5})
    #: Whether this channel contributes to the cross-source borsa consensus.
    #: False for exchange-office channels, whose quotes are a different
    #: market layer even when they occasionally report a wholesale figure.
    in_consensus: bool = True


# ---------------------------------------------------------------------------
# City dictionary
#
# Patterns are written against NORMALIZED text (see fx/normalize.py), so they
# assume: no kashida, taa marbuta folded to haa, hamzated alefs folded to bare
# alef, Persian yeh/kaf folded to Arabic. That is why every entry below spells
# Basra as "بصره" and Harithiya as "حارثيه" -- those are the folded forms that
# match every variant the channels actually use.
#
# Order matters: most specific first. A match consumes its span so a later,
# broader pattern (bare "بغداد") cannot reuse it.
# ---------------------------------------------------------------------------

_AL = r"(?:ال)?"
#: Optional "بغداد -" prefix in front of a named Baghdad sub-market.
_BGD = r"(?:بغداد\s*[-/]?\s*)?"

CITY_PATTERNS = [
    # --- Baghdad sub-markets (most specific first) -------------------------
    # Compound market names. "شورجة حارثية" and "كرادة حارثية"
    # are single quotes naming a trading corridor, not two separate readings;
    # both are booked to Harithiya, the market that actually sets the price.
    # A "بغداد - " prefix is absorbed by the specific market so that
    # "بغداد - بورصة الكفاح" produces one Kifah reading, not a Kifah reading
    # plus a generic-Baghdad reading sharing the same number.
    ("baghdad_harithiya", rf"(?:{_AL}شورجه|{_AL}كراده)\s+{_AL}حارثيه"),
    ("baghdad_kifah", rf"{_BGD}(?:بورصه\s*)?{_AL}كفاح\s*-\s*{_AL}سموال"),
    ("baghdad_kifah", rf"{_BGD}(?:بورصه\s*)?{_AL}كفاح"),
    ("baghdad_harithiya", rf"{_BGD}(?:بورصه\s*)?{_AL}حارثيه"),
    ("baghdad_shorja", rf"{_BGD}{_AL}شورجه"),
    ("baghdad_karrada", rf"{_BGD}{_AL}كراده"),
    ("baghdad_samawal", rf"{_BGD}{_AL}سموال"),
    # Retail / exchange-office layers inside Baghdad. @dollar_price's 2018-19
    # tables append "السعر في الصيرفات ( تقريبي)" under the Baghdad borsa
    # figure; capturing it here keeps those numbers out of the borsa series.
    ("baghdad_retail", r"بغداد\s*[/\-]?\s*صيرفات"),
    ("baghdad_retail", rf"مكاتب\s*(?:{_AL}صيرفه\s*)?(?:بغداد)?"),
    ("baghdad_retail", r"(?:السعر\s*)?في\s*{0}صيرفات".format(_AL)),
    ("baghdad_generic", r"بغداد(?:\s*عام)?(?:\s*[-/]?\s*(?:ال)?بورصه)?"),

    # --- Kurdistan ---------------------------------------------------------
    # Erbil's retail/sub-market labels (اسكان/ايسكان/خمسات/صيرفات) must come
    # before the bare city or they collapse into the wholesale series.
    # "ه{1,2}ولير" covers both the Arabic "هولير" and the Kurdish "ههولێر"
    # (folded to "ههولير"). "سليماني" appears with and without the final haa.
    # خمسات / پێنجي ("fifties") denote a quote for $50 notes, which trades at
    # a different price from hundreds -- held out of the wholesale series.
    ("erbil_retail",
     r"(?:ا?ربيل|ه{1,2}ولير)\s*[-/]?\s*(?:ا?يسكان|اسكان|خمسات|پينجي|صيرفات)"),
    ("erbil", r"ا?ربيل|ه{1,2}ولير"),
    ("sulaymaniyah", rf"{_AL}سليمانيه?"),
    ("dohuk", r"دهوك"),
    ("zakho", r"زاخو"),
    ("halabja", r"حلبجه"),

    # --- Combined headers: one price covering several cities ---------------
    # @dollar_price posts "الموصل - اربيل - سليمانية - دهوك" over a single
    # figure; each city gets that figure.
    ("__najaf_karbala__", r"النجف\s*-\s*كربلاء"),
    ("__karbala_babil__", r"كربلاء\s*وبابل"),

    # --- Rest of the country ----------------------------------------------
    ("karbala", r"كربلاء"),
    ("najaf", rf"{_AL}نجف(?:\s*الاشرف)?"),
    ("basra", rf"{_AL}بصره"),
    ("kirkuk", r"كركوك"),
    ("mosul", rf"{_AL}موصل"),
    ("ramadi", rf"{_AL}رمادي|{_AL}انبار|{_AL}فلوجه"),
    ("samawah", rf"{_AL}سماوه"),
    ("nasiriyah", rf"{_AL}ناصريه"),
    ("baqubah", r"بعقوبه"),
    ("hillah", rf"{_AL}حله"),
    ("babil", r"بابل"),
    ("kut", rf"{_AL}كوت"),
    ("amarah", rf"{_AL}عماره|ميسان"),
    ("diwaniyah", rf"{_AL}ديوانيه"),
    ("tikrit", r"تكريت|صلاح\s*الدين"),
    ("samarra", r"سامراء"),
    ("rutba", rf"{_AL}رطبه"),
]

# ---------------------------------------------------------------------------
# What the dataset publishes
#
# The published panel is deliberately narrow: Baghdad, Basra and Erbil, the
# three markets with continuous multi-year coverage. Each is published as its
# own series. No aggregate across the three is produced -- an unweighted mean
# of three cities corresponds to nothing traded and would give Baghdad, by far
# the largest market, the same weight as the other two. Anyone wanting an
# aggregate can build one from the published columns with a weighting they can
# justify.
# ---------------------------------------------------------------------------

#: Baghdad sub-market priority. The published Baghdad series is the highest
#: priority sub-market quoted on the day; the rest are fallbacks.
#:
#: Kifah is first because it is the price-setting wholesale borsa in Baghdad
#: and the channels themselves treat it as the headline -- e.g. @dollar_price
#: 2023-04-01 announces "the dollar in Baghdad is below 150 thousand" and
#: prints the Kifah figure as the evidence.
#:
#: The fallback order is fixed a priori, never chosen per day. Measured on the
#: full corpus the sub-markets are the same price to within a rounding tick --
#: Kifah minus Harithiya has a median difference of 0.00 IQD (n=606), Kifah
#: minus Generic 0.50 IQD / 0.034% (n=1,025), and no pair exceeds 0.86 IQD --
#: so the fallback introduces no material level shift. See
#: METHODOLOGY_AS_BUILT.md for the full table.
BAGHDAD_PRIORITY = ["baghdad_kifah", "baghdad_generic", "baghdad_harithiya",
                    "baghdad_karrada", "baghdad_samawal", "baghdad_shorja"]

#: The published markets outside Baghdad.
NON_BAGHDAD_MARKETS = ["basra", "erbil"]

#: Every city series the published dataset carries.
PUBLISHED_CITIES = BAGHDAD_PRIORITY + NON_BAGHDAD_MARKETS

# ---------------------------------------------------------------------------
# Recognised-but-discarded labels
#
# THESE PATTERNS MUST STAY IN CITY_PATTERNS EVEN THOUGH THEIR READINGS ARE
# THROWN AWAY. `scan_city_prices` clips each city's price-search window at the
# neighbouring city names, so a label is what stops one market from claiming
# another market's number. Deleting the pattern does not remove the price from
# the post -- it orphans it, and the nearest surviving city absorbs it.
#
# Measured: deleting these entries from CITY_PATTERNS and re-parsing the whole
# corpus fabricates 973 readings and changes 168 more in the RETAINED cities
# (baghdad_generic +619, erbil +490, basra +22, baghdad_kifah +9). Almost all
# of them are retail quotes migrating into the wholesale series. Recognising
# the label and dropping the reading gives the intended output with none of
# that contamination.
# ---------------------------------------------------------------------------

#: Readings for these cities are discarded immediately after extraction.
DISCARDED_CITIES = {
    # Retail / exchange-office layer -- a counter rate is a markup over the
    # wholesale price, not another observation of it.
    "baghdad_retail", "erbil_retail",
    # Regional markets removed from the published panel.
    "kirkuk", "karbala", "najaf", "mosul", "sulaymaniyah", "dohuk",
}

ALL_CITY_KEYS = sorted({k for k, _ in CITY_PATTERNS if not k.startswith("__")})

#: City keys that can still appear in a reading. The discarded labels are
#: recognised during extraction but never survive it, so carrying columns for
#: them through the daily tables would only produce permanently empty columns.
SERIES_CITY_KEYS = [k for k in ALL_CITY_KEYS if k not in DISCARDED_CITIES]


def pick_baghdad(available):
    """Resolve the Baghdad open/close pair from whatever sub-markets quoted.

    `available` maps a sub-market key to its (open, close) pair for one day.
    Returns (open, close, source) where `source` names the sub-market the
    numbers came from, or (None, None, None) if Baghdad was not quoted.

    One rule, applied identically on every day: take the highest-priority
    sub-market present. Open and close are always taken from the SAME
    sub-market, so the intraday change is a real change in one market rather
    than an artefact of two markets being spliced together within a day.

    This replaces the earlier mean over whichever sub-markets happened to post.
    That mean was a basket whose composition changed daily -- one series on 972
    days, two on 519, up to six on 28 -- so its definition was not constant
    through the sample. The two definitions are empirically interchangeable
    (median absolute error against the CBI market price 0.232% for this rule
    versus 0.226% for the mean, on 1,224 overlapping days); the reason to
    prefer this one is that it is a single stated definition with 100% coverage
    of the Baghdad days, not that it fits better.
    """
    for key in BAGHDAD_PRIORITY:
        pair = available.get(key)
        if pair and pair[1] is not None:
            return pair[0], pair[1], key
    return None, None, None

#: Expansion of the combined-city header keys.
COMBINED_CITIES = {
    "__najaf_karbala__": ["najaf", "karbala"],
    "__karbala_babil__": ["karbala", "babil"],
}

COMPILED_CITIES = [(k, re.compile(p)) for k, p in CITY_PATTERNS]


# ==========================================================================
# EXTRACTION  —  pairing a city name with the nearest price
# ==========================================================================

# ---------------------------------------------------------------------------
# Side markers
#
#
# Longest-first ordering matters: "طلب" must be tried before the bare "ط".
# ---------------------------------------------------------------------------

_SIDE_TOKENS = [
    (SIDE_TRADED, r"انباع"),
    (SIDE_BID, r"مطلوب"),
    (SIDE_BID, r"طلب"),
    (SIDE_OFFER, r"عرض"),
    (SIDE_SELL, r"(?:سعر\s*)?(?:ال)?بيع"),
    (SIDE_BUY, r"(?:سعر\s*)?(?:ال)?شراء"),
    # Single-letter markers, standalone only: "ع" and "ط" are ordinary Arabic
    # letters and would otherwise match inside every other word.
    (SIDE_BID, r"(?<![ء-ي])ط(?![ء-ي])"),
    (SIDE_OFFER, r"(?<![ء-ي])ع(?![ء-ي])"),
]

_COMPILED_SIDES = [(s, re.compile(p)) for s, p in _SIDE_TOKENS]

#: Qualifiers that trail a side marker and must never be read as one:
#: احمر/خمسات/شدة describe the banknotes, باجر/الان the settlement timing.
SIDE_NOISE_RE = re.compile(
    r"احمر|خمسات|خمسينات|مسلفن|جاهز|باجر|كميات|عشرات|سور|الان|شده|قفل")


def detect_side(context: str, default: str = SIDE_UNKNOWN) -> str:
    """Return the side implied by the text around a price.

    When a post quotes both sides ("146.650 ع ط احمر") the earliest marker
    wins, which is the ask — consistent with an ask-based headline series.
    """
    best = None
    for side, pat in _COMPILED_SIDES:
        m = pat.search(context)
        if m and (best is None or m.start() < best[0]):
            best = (m.start(), side)
    return best[1] if best else default


# ---------------------------------------------------------------------------
# City span resolution
# ---------------------------------------------------------------------------

#: A gap that merely joins two names in a shared-price run: "الموصل - اربيل".
#: An explicit joiner is REQUIRED. Whitespace alone is not enough, because a
#: bare newline between two markets usually means the first simply had no
#: price posted, not that the two share one.
_RUN_GAP_RE = re.compile(r"^[\s]*(?:[-–—/،,]|و)[\s]*$")


def city_spans(text: str) -> List[Tuple[int, int, str]]:
    """All city-name occurrences, resolved by dictionary priority.

    Patterns are tried in CITY_PATTERNS order and each match claims its
    characters, so the specific "بورصة الكفاح" wins over the generic "بغداد"
    and the generic cannot re-match the same words.
    """
    claimed = [False] * len(text)
    spans: List[Tuple[int, int, str]] = []
    for key, rx in COMPILED_CITIES:
        for m in rx.finditer(text):
            if any(claimed[m.start():m.end()]):
                continue
            for i in range(m.start(), m.end()):
                claimed[i] = True
            spans.append((m.start(), m.end(), key))
    spans.sort()
    return spans


def _group_runs(text: str, spans, share_runs: bool):
    """Collapse "city - city - city" runs that share a single price."""
    if not spans:
        return []
    groups = [[spans[0]]]
    for span in spans[1:]:
        prev = groups[-1][-1]
        gap = text[prev[1]:span[0]]
        if share_runs and _RUN_GAP_RE.match(gap):
            groups[-1].append(span)
        else:
            groups.append([span])
    return groups


def _expand(key: str) -> List[str]:
    return COMBINED_CITIES.get(key, [key])


# ---------------------------------------------------------------------------
# Proximity scanner
# ---------------------------------------------------------------------------

def scan_city_prices(text: str,
                     *,
                     price_pattern: str = PRICE_DEFAULT,
                     window: int = 30,
                     side_window: int = 14,
                     default_side: str = SIDE_UNKNOWN,
                     detect_sides: bool = False,
                     share_runs: bool = False,
                     layer: str = LAYER_BORSA,
                     matcher_id: str = "") -> List[Reading]:
    """Pair each city (or run of cities) with the nearest unclaimed price.

    Matching runs in both directions because some eras put the number first
    ("162.100 بغداد") and others last ("بغداد 162.100").

    The search window is clipped to the neighbouring city names. Without that
    clipping a label with no price of its own steals the next market's number:

        كفاح
        حارثية ط 162.000

    would otherwise book 162.000 to Kifah as well as Harithiya.
    """
    price_re = re.compile(price_pattern)
    groups = _group_runs(text, city_spans(text), share_runs)

    found: List[Reading] = []
    seen = set()
    consumed = [False] * len(text)

    def free(a, b):
        return not any(consumed[a:b])

    def consume(a, b):
        for i in range(max(0, a), min(len(text), b)):
            consumed[i] = True

    for gi, group in enumerate(groups):
        g_start, g_end = group[0][0], group[-1][1]

        # Clip the search window at the neighbouring groups.
        left_bound = groups[gi - 1][-1][1] if gi else 0
        right_bound = groups[gi + 1][0][0] if gi + 1 < len(groups) else len(text)

        after_start, after_end = g_end, min(g_end + window, right_bound)
        before_start = max(g_start - window, left_bound)

        after = text[after_start:after_end]
        before = text[before_start:g_start]

        # Collect every candidate on both sides, nearest first, rather than
        # committing to the single closest one. In a table such as
        #
        #     بغداد - بورصة الكفاح / 147,200 / السعر في الصيرفات / 146,750
        #
        # the retail label's nearest number is the Kifah price sitting just
        # behind it -- already claimed. Falling back to the next candidate
        # recovers the retail figure instead of dropping the reading.
        candidates = []
        for m in price_re.finditer(after):
            candidates.append((m.start(), m.group(0),
                               after_start + m.start(), after_start + m.end()))
        for m in price_re.finditer(before):
            candidates.append((len(before) - m.end(), m.group(0),
                               before_start + m.start(), before_start + m.end()))
        candidates.sort(key=lambda c: c[0])

        chosen = None
        for _, token, p_start, p_end in candidates:
            if not free(p_start, p_end):
                continue
            rate = to_rate(token)
            if rate is None:
                continue
            chosen = (rate, p_start, p_end)
            break

        if chosen is None:
            continue

        rate, p_start, p_end = chosen
        consume(p_start, p_end)

        side = default_side
        if detect_sides:
            ctx = text[max(0, p_start - side_window): p_end + side_window]
            side = detect_side(ctx, default=default_side)

        for _, _, key in group:
            for city in _expand(key):
                if (city, side) in seen:
                    continue
                seen.add((city, side))
                found.append(Reading(city=city, rate=rate, side=side,
                                     layer=layer, matcher=matcher_id))

    return found


def city_blocks(text: str) -> Iterator[Tuple[str, str]]:
    """Yield (city_key, text_following_that_city) for each city label.

    Used where one post holds several cities that each carry a labelled
    buy/sell pair, so proximity alone cannot tell which pair belongs to which
    city (@dollar_price's 2024-25 era).
    """
    spans = city_spans(text)
    for i, (_, end, key) in enumerate(spans):
        stop = spans[i + 1][0] if i + 1 < len(spans) else len(text)
        yield key, text[end:stop]


# ---------------------------------------------------------------------------
# Anchored label extraction
# ---------------------------------------------------------------------------

def region_between(text: str,
                   start_pattern: Optional[str],
                   end_pattern: Optional[str]) -> Optional[str]:
    """Slice out the region delimited by two patterns.

    Isolates @Kukh_alomlat's USD block from the eight foreign-currency blocks
    that follow it. Returns None when the start marker is absent, so the
    profile falls through to the next matcher instead of guessing.
    """
    start = 0
    if start_pattern:
        m = re.search(start_pattern, text)
        if not m:
            return None
        start = m.end()

    end = len(text)
    if end_pattern:
        m = re.search(end_pattern, text[start:])
        if m:
            end = start + m.start()
    return text[start:end]


def labelled_value(region: str, label_pattern: str,
                   price_pattern: str = PRICE_DEFAULT,
                   max_gap: int = 25) -> Optional[float]:
    """First plausible price within `max_gap` characters after a label."""
    price_re = re.compile(price_pattern)
    for m in re.finditer(label_pattern, region):
        tail = region[m.end(): m.end() + max_gap]
        pm = price_re.search(tail)
        if pm:
            rate = to_rate(pm.group(0))
            if rate is not None:
                return rate
    return None


# ==========================================================================
# CHANNEL FORMATS  —  contamination patterns shared by all channels
# ==========================================================================

# ---------------------------------------------------------------------------
# Contamination patterns shared by several channels.
#
# STRIP_*  : blanked out before matching, so the numbers inside cannot be read
#            as rates while the rest of the post is still parsed.
# EXCLUDE_*: drop the whole post, with a reason code recorded for auditing.
# ---------------------------------------------------------------------------

# Gold is quoted per mithqal and labelled by city exactly like FX, so a
# gold-focused post looks structurally identical to a rate post.
STRIP_GOLD = re.compile(
    r"(?:مثقال\s*)?(?:ال)?ذهب[^\n]{0,80}|عيار\s*\d+[^\n]{0,60}|اونصه[^\n]{0,60}")

# Foreign-currency figures. Some, like "2,585,000 تومان", embed a run of
# digits that reads as a plausible IQD rate if left in place.
STRIP_FOREIGN = re.compile(
    r"[^\n]{0,20}(?:يورو|اليورو|باوند|باون|استرليني|ليره|توما?ن|تمن|"
    r"ريال|روبيه|درهم|دينار\s*اردني|كندي|استرالي|فرنك|درهم\s*اماراتي)[^\n]{0,40}")

# Central-bank window/auction posts report volumes in millions of dollars,
# not street rates.
STRIP_CBI = re.compile(
    r"[^\n]{0,30}(?:نافذه\s*(?:بيع\s*)?العمله|مزاد\s*البنك\s*المركزي|"
    r"مبيعات\s*البنك\s*المركزي|الاحتياطي\s*النقدي)[^\n]{0,80}")

# "بنسبة (50%)", "93%" — percentages sit next to plausible-looking integers.
STRIP_PERCENT = re.compile(r"[^\s]{0,12}\d+\s*[%٪][^\n]{0,20}")

# Barrel prices, reserves, GDP: "59$ للبرميل", "113 مليار دولار".
STRIP_MACRO = re.compile(
    r"[^\n]{0,30}(?:للبرميل|برميل|مليار\s*دولار|مليون\s*دولار|تريليون)[^\n]{0,40}")

# Traveller FX allowances: "حصة مسافرين الى 2000$", "4000$".
STRIP_ALLOWANCE = re.compile(r"[^\n]{0,40}(?:مسافرين|حصه\s*المسافر)[^\n]{0,40}")

# Banknote bundle counts sitting on their own line next to a price:
# "40 شدة", "20 شدة".
STRIP_BUNDLES = re.compile(r"\d{1,3}\s*شده")

# Dates written in-body: "13/7/2023", "25-5-2019", "ليوم الاحد 13-9-2020".
STRIP_DATES = re.compile(r"\d{1,2}\s*[-/]\s*\d{1,2}\s*[-/]\s*\d{2,4}")

# Clock lines: "الساعة 11:45 ص", "ساعة 2:35 مساء".
STRIP_CLOCK = re.compile(r"(?:ال)?ساعه\s*:?\s*\d{1,2}\s*:\s*\d{2}[^\n]{0,12}")

#: Safe to apply almost everywhere.
STRIP_COMMON = [STRIP_DATES, STRIP_CLOCK, STRIP_PERCENT]


EXCLUDE_POLL = ("poll", re.compile(r"Anonymous Poll|\bvoters\b", re.I))
EXCLUDE_LINK_ONLY = ("link_only", re.compile(r"^\s*$"))


_REGISTRY = None


# ==========================================================================
# CHANNEL FORMAT  —  @dollariraqi
# ==========================================================================

# Feb-Mar 2021: rates quoted directly in IQD per 1 USD under one of these
# headers, so the usual per-100 scaling must not be applied.
DOLLARIRAQI_DIRECT_HEADER_RE = re.compile(r"التصريف\s*المحلي|بورصه\s*العراق\s*الماليه")
DOLLARIRAQI_DIRECT_PRICE_RE = re.compile(r"(?<![\d])\d{4}(?:\.\d{1,2})?(?![\d])")

# The official CBI figure is announced as "100$ = 1,320 دينار"; it is a policy
# rate, not a street rate, and is stripped rather than parsed.
DOLLARIRAQI_STRIP_OFFICIAL = re.compile(r"السعر\s*الرسمي[^\n]{0,60}")


def DOLLARIRAQI_direct_scale(text: str) -> List[Reading]:
    """Feb-Mar 2021 era: figures are already IQD per 1 USD."""
    if not DOLLARIRAQI_DIRECT_HEADER_RE.search(text):
        return []
    out, seen = [], set()
    for key, city_re in COMPILED_CITIES:
        if key.startswith("__"):
            continue
        m = city_re.search(text)
        if not m:
            continue
        pm = DOLLARIRAQI_DIRECT_PRICE_RE.search(text[m.end(): m.end() + 20])
        if not pm:
            continue
        val = float(pm.group(0))
        if not (MIN_PLAUSIBLE_RATE <= val <= MAX_PLAUSIBLE_RATE):
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(Reading(city=key, rate=val, side=SIDE_UNKNOWN,
                           layer=LAYER_BORSA, matcher="dollariraqi:direct_scale_2021"))
    return out


# The channel's second daily post reports the Baghdad exchange offices rather
# than the borsa: "اسعار مكاتب الصيرفة : سعر بيع الدولار 126,000 / سعر شراء
# الدولار 125,000". It names no city, so the proximity scanner never saw it --
# these were the 84 posts the original parser left unmatched. The tail of the
# post carries world FX, metals and oil quotes, so the region is cut at the
# first of those headers.
DOLLARIRAQI_RETAIL_SIG = re.compile(r"مكاتب\s*(?:ال)?صيرفه")
DOLLARIRAQI_RETAIL_END = r"العملات\s*العالميه|المعادن|النفط|الذهب"
DOLLARIRAQI_SELL_LABEL = r"(?:سعر\s*)?(?:ال)?بيع(?:\s*الدولار)?|تبيع"
DOLLARIRAQI_BUY_LABEL = r"(?:سعر\s*)?(?:ال)?شراء(?:\s*الدولار)?|تشتري"


def DOLLARIRAQI_retail_office(text: str) -> List[Reading]:
    region = region_between(text, r"مكاتب\s*(?:ال)?صيرفه", DOLLARIRAQI_RETAIL_END)
    if region is None:
        return []
    out = []
    sell = labelled_value(region, DOLLARIRAQI_SELL_LABEL, max_gap=30)
    buy = labelled_value(region, DOLLARIRAQI_BUY_LABEL, max_gap=30)
    for rate, side in ((sell, SIDE_SELL), (buy, SIDE_BUY)):
        if rate is not None:
            out.append(Reading(city="baghdad_retail", rate=rate, side=side,
                               layer=LAYER_RETAIL,
                               matcher="dollariraqi:retail_office"))
    return out


def DOLLARIRAQI_table(text: str) -> List[Reading]:
    """Multi-city borsa table, plus the retail band many tables carry.

    A table often ends with "مكاتب الصيرفة بغداد ( تقريبي ) / تشتري 125.000 /
    تبيع 126.000". The proximity scanner can only pair that label with one of
    the two numbers, so the labelled buy/sell pair is substituted in when it
    is available -- keeping both sides of the retail quote instead of an
    arbitrary one.
    """
    readings = scan_city_prices(
        text,
        price_pattern=PRICE_DEFAULT,
        window=30,
        layer=LAYER_BORSA,
        matcher_id="dollariraqi:city_table",
    )
    pair = DOLLARIRAQI_retail_office(text)
    if pair:
        readings = [r for r in readings if r.city != "baghdad_retail"] + pair
    return readings


# Posts that quote a rate without naming any city. Three recurring forms, all
# well anchored enough to read safely:
#
#   السعر في البورصة 149,000                    -> Baghdad borsa
#   100 $ = 127,500 دينار عراقي                 -> Baghdad borsa (the headline
#      line of a cross-rate table whose other lines are EUR/GBP/JPY/AED)
#   اسعار الصرف في عموم العراق / الشراء / البيع -> nationwide office rates
#   سعر صرف ال100 دولار في اغلب الصيرفات        -> nationwide office rates
DOLLARIRAQI_GENERAL_SIG = re.compile(
    r"السعر\s*في\s*(?:ال)?بورصه|100\s*\$\s*=|عموم\s*العراق|اغلب\s*(?:ال)?صيرفات")
DOLLARIRAQI_BORSA_ONLY_RE = re.compile(r"السعر\s*في\s*(?:ال)?بورصه\s*(" + PRICE_DEFAULT + ")")
DOLLARIRAQI_PER100_IQD_RE = re.compile(
    r"100\s*\$\s*=\s*(" + PRICE_DEFAULT + r")\s*دينار\s*عراقي")
DOLLARIRAQI_OFFICES_SIG = re.compile(r"عموم\s*العراق|اغلب\s*(?:ال)?صيرفات")


def DOLLARIRAQI_general_market(text: str) -> List[Reading]:

    for rx in (DOLLARIRAQI_BORSA_ONLY_RE, DOLLARIRAQI_PER100_IQD_RE):
        m = rx.search(text)
        if m:
            rate = to_rate(m.group(1))
            if rate is not None:
                return [Reading(city="baghdad_generic", rate=rate,
                                side=SIDE_UNKNOWN, layer=LAYER_BORSA,
                                matcher="dollariraqi:general_market")]

    if DOLLARIRAQI_OFFICES_SIG.search(text):
        out = []
        sell = labelled_value(text, DOLLARIRAQI_SELL_LABEL, max_gap=30)
        buy = labelled_value(text, DOLLARIRAQI_BUY_LABEL, max_gap=30)
        for rate, side in ((sell, SIDE_SELL), (buy, SIDE_BUY)):
            if rate is not None:
                out.append(Reading(city="baghdad_retail", rate=rate, side=side,
                                   layer=LAYER_RETAIL,
                                   matcher="dollariraqi:general_market"))
        return out
    return []


DOLLARIRAQI_PROFILE = ChannelProfile(
    channel="dollariraqi",
    title="سعر الدولار في العراق لحظة بلحظة",
    default_layer=LAYER_BORSA,
    strips=[STRIP_GOLD, STRIP_FOREIGN, DOLLARIRAQI_STRIP_OFFICIAL, STRIP_CBI,
            STRIP_DATES, STRIP_CLOCK, STRIP_PERCENT],
    excludes=[
        # A gold-focused post labels prices by city exactly like an FX post,
        # so anything leading with "ذهب" is dropped whole rather than stripped.
        # Gold is quoted per gram and per mithqal in the same shape as FX.
        ("gold_post", re.compile(r"^.{0,30}ذهب|سعر\s*الغرام|المثقال|الصياغه")),
        ("cbi_window", re.compile(r"نافذه\s*(?:بيع\s*)?العمله")),
        ("news_link", re.compile(r"https?://|التفاصي")),
    ],
    matchers=[
        Matcher(id="dollariraqi:direct_scale_2021", fn=DOLLARIRAQI_direct_scale,
                signature=DOLLARIRAQI_DIRECT_HEADER_RE,
                note="Feb-Mar 2021, rates in IQD per 1 USD"),
        Matcher(id="dollariraqi:city_table", fn=DOLLARIRAQI_table,
                note="multi-city table, IQD per 100 USD"),
        Matcher(id="dollariraqi:retail_office", fn=DOLLARIRAQI_retail_office,
                signature=DOLLARIRAQI_RETAIL_SIG,
                note="Baghdad exchange-office buy/sell post, no city named"),
        Matcher(id="dollariraqi:general_market", fn=DOLLARIRAQI_general_market,
                signature=DOLLARIRAQI_GENERAL_SIG,
                note="rate quoted with no city named at all"),
    ],
)


# ==========================================================================
# CHANNEL FORMAT  —  @iqborsa
# ==========================================================================

IQBORSA_BUY_SELL_SIG = re.compile(r"سعر\s*الصرف\s*الحالي")
IQBORSA_OPEN_TABLE_SIG = re.compile(r"افتتاح\s*البورصه")
IQBORSA_DOLLAR_LABEL_SIG = re.compile(r"(?:^|\n)\s*الدولار\s*:")

#: Post is short enough that a bare 3-digit number is safely a price.
IQBORSA_SHORT_POST = 60


def IQBORSA_buy_sell(text: str) -> List[Reading]:
    """Era A: 'سعر الصرف الحالي (بغداد )' / 'البيع 148.000' / 'الشراء 147.500'."""
    region = region_between(text, r"سعر\s*الصرف\s*الحالي", r"ملاحظه")
    if region is None:
        return []
    out = []
    sell = labelled_value(region, r"(?:سعر\s*)?(?:ال)?بيع")
    buy = labelled_value(region, r"(?:سعر\s*)?(?:ال)?شراء")
    for rate, side in ((sell, SIDE_SELL), (buy, SIDE_BUY)):
        if rate is not None:
            out.append(Reading(city="baghdad_generic", rate=rate, side=side,
                               layer=LAYER_BORSA,
                               matcher="iqborsa:buy_sell_block"))
    return out


IQBORSA_BARE_PAIR_SIG = re.compile(r"(?:^|\n)\s*بيع\s*:")


def IQBORSA_bare_buy_sell(text: str) -> List[Reading]:
    """Late-2024 era: a two-line quote with no city and no header at all.

        بيع : 150.500
        شراء : 149.700

    Distinct from the era-A block, which is introduced by 'سعر الصرف الحالي'.
    """
    if len(text) > 90:
        return []
    out = []
    sell = labelled_value(text, r"(?:^|\n)\s*(?:سعر\s*)?(?:ال)?بيع\s*:", max_gap=20)
    buy = labelled_value(text, r"(?:^|\n)\s*(?:سعر\s*)?(?:ال)?شراء\s*:", max_gap=20)
    for rate, side in ((sell, SIDE_SELL), (buy, SIDE_BUY)):
        if rate is not None:
            out.append(Reading(city="baghdad_generic", rate=rate, side=side,
                               layer=LAYER_BORSA,
                               matcher="iqborsa:bare_buy_sell"))
    return out


def IQBORSA_open_table(text: str) -> List[Reading]:
    """Era A: the daily multi-city opening table."""
    return scan_city_prices(text, price_pattern=PRICE_DEFAULT, window=30,
                            layer=LAYER_BORSA,
                            matcher_id="iqborsa:opening_table")


def IQBORSA_dollar_label(text: str) -> List[Reading]:
    """Era D: 'الــدولار : 147.750' — no city, means Baghdad.

    The companion gold post has the identical shape ('الذهــب : 675 الف'),
    which the gold strip removes before this matcher runs.
    """
    m = re.search(r"الدولار\s*:?\s*(" + PRICE_DEFAULT + ")", text)
    if not m:
        return []
    rate = to_rate(m.group(1))
    if rate is None:
        return []
    return [Reading(city="baghdad_generic", rate=rate, side=SIDE_UNKNOWN,
                    layer=LAYER_BORSA, matcher="iqborsa:dollar_label")]


def IQBORSA_city_line(text: str) -> List[Reading]:
    """Eras B, C, F and the labelled part of E."""
    return scan_city_prices(text, price_pattern=PRICE_DEFAULT, window=30,
                            layer=LAYER_BORSA,
                            matcher_id="iqborsa:city_line")


def IQBORSA_bare_number(text: str) -> List[Reading]:
    """Era E: the post is just a price, with no label at all.

    The padding around the number may contain emoji and punctuation but NOT
    letters. Allowing letters made this matcher read "جماعة ال200 ولك" and
    "خام برنت : 109 $" as Baghdad rates: a bare 3-digit number is only a
    plausible price when there is nothing else in the message it could be
    counting.
    """
    if len(text) > IQBORSA_SHORT_POST:
        return []
    m = re.fullmatch(r"[^\w\d]{0,14}(" + PRICE_LOOSE + r")[^\w\d]{0,14}", text)
    if not m:
        return []
    rate = to_rate(m.group(1))
    if rate is None:
        return []
    return [Reading(city="baghdad_generic", rate=rate, side=SIDE_UNKNOWN,
                    layer=LAYER_BORSA, matcher="iqborsa:bare_number")]


def IQBORSA_short_city_line(text: str) -> List[Reading]:
    """Era E: 'بغداد 151' — a bare 3-digit price behind a city label.

    Kept very tight (30 characters). At 60 it swallowed
    "درجة الحرارة تبلغ 114 ف في العاصمة بغداد" as a Baghdad quote.
    """
    if len(text) > 30:
        return []
    return scan_city_prices(text, price_pattern=PRICE_LOOSE, window=20,
                            layer=LAYER_BORSA,
                            matcher_id="iqborsa:short_city_line")


IQBORSA_PROFILE = ChannelProfile(
    channel="iqborsa",
    title="بيش الدولار اليوم",
    default_layer=LAYER_BORSA,
    strips=[STRIP_GOLD, STRIP_FOREIGN, STRIP_CBI, STRIP_MACRO,
            STRIP_ALLOWANCE, STRIP_DATES, STRIP_CLOCK, STRIP_PERCENT],
    excludes=[
        ("poll", re.compile(r"Anonymous Poll|\bvoters\b", re.I)),
        ("breaking_news", re.compile(r"عاجل")),
        # Attributed statements: "البنك المركزي :", "خبير اقتصادي :".
        ("attributed_news", re.compile(
            r"(?:البنك\s*المركزي|خبير\s*اقتصادي|مجلس\s*الوزراء|وزاره|"
            r"رئيس\s*الوزراء|النائب|مصادر|رويترز)\s*:")),
        # Wire copy credited to an outlet. Long prose posts often contain both
        # a city name and a number, which the city_line matcher will happily
        # pair up: "نون بوست : شنت الإمارات ... مصفاة" produced a 1840 quote.
        ("wire_copy", re.compile(
            r"(?:نون\s*بوست|بوست|وكاله|وكالة|صحيفه|قناه|CNN|BBC|"
            r"الجزيره|السومريه|شفق\s*نيوز)\s*:", re.I)),
        ("mailbag", re.compile(r"من\s*البريد\s*:|تبادل\s*خبرات")),
        ("subscriber_milestone", re.compile(r"الف\s*مشترك|THANKS FOR ALL", re.I)),
        ("cross_promo", re.compile(r"اضغط\s*الانضمام|t\.me/shakoiq", re.I)),
        # The historical-comparison post is the nastiest trap in the channel:
        # it lists in-band rates for 2006-2024 that would all parse cleanly.
        ("historical_comparison", re.compile(r"في\s*20\d\d\s*:")),
        ("gold_post", re.compile(r"^.{0,30}ذهب")),
    ],
    matchers=[
        Matcher(id="iqborsa:buy_sell_block", fn=IQBORSA_buy_sell,
                signature=IQBORSA_BUY_SELL_SIG, note="era A, explicit بيع/شراء"),
        Matcher(id="iqborsa:bare_buy_sell", fn=IQBORSA_bare_buy_sell,
                signature=IQBORSA_BARE_PAIR_SIG,
                note="late 2024, 'بيع : X' / 'شراء : Y' with no city"),
        Matcher(id="iqborsa:opening_table", fn=IQBORSA_open_table,
                signature=IQBORSA_OPEN_TABLE_SIG, note="era A, daily opening table"),
        Matcher(id="iqborsa:dollar_label", fn=IQBORSA_dollar_label,
                signature=IQBORSA_DOLLAR_LABEL_SIG, note="era D, 'الدولار : 147.750'"),
        Matcher(id="iqborsa:city_line", fn=IQBORSA_city_line,
                note="eras B/C/F, city + price on one line"),
        Matcher(id="iqborsa:short_city_line", fn=IQBORSA_short_city_line,
                note="era E, 'بغداد 151'"),
        Matcher(id="iqborsa:bare_number", fn=IQBORSA_bare_number,
                note="era E, price with no label"),
    ],
)


# ==========================================================================
# CHANNEL FORMAT  —  @dollar_price
# ==========================================================================

DOLLAR_PRICE_NARRATIVE_SIG = re.compile(r"سعر\s*صرف\s*100\s*دولار")
DOLLAR_PRICE_BUY_SELL_SIG = re.compile(r"سعر\s*البيع|سعر\s*الشراء")

DOLLAR_PRICE_SHORT_POST = 80


def DOLLAR_PRICE_narrative_2017(text: str) -> List[Reading]:
    """Era A: one prose sentence carrying the USD figure."""
    m = re.search(r"100\s*دولار\s*اميركي\s*بلغ\s*(" + PRICE_DEFAULT + ")", text)
    if not m:
        return []
    rate = to_rate(m.group(1))
    if rate is None:
        return []
    # The city is named on its own line above the sentence; fall back to
    # Basra, the only city this era ever covered.
    city = "basra"
    for key, _ in city_blocks(text):
        if not key.startswith("__"):
            city = key
            break
    return [Reading(city=city, rate=rate, side=SIDE_UNKNOWN,
                    layer=LAYER_BORSA, matcher="dollar_price:narrative_2017")]


def DOLLAR_PRICE_city_buy_sell(text: str) -> List[Reading]:
    """Era E: per-city blocks, each with 'سعر البيع' and 'سعر الشراء'."""
    out = []
    for key, block in city_blocks(text):
        if key.startswith("__"):
            continue
        sell = labelled_value(block, r"(?:سعر\s*)?(?:ال)?بيع")
        buy = labelled_value(block, r"(?:سعر\s*)?(?:ال)?شراء")
        for rate, side in ((sell, SIDE_SELL), (buy, SIDE_BUY)):
            if rate is not None:
                out.append(Reading(city=key, rate=rate, side=side,
                                   layer=LAYER_BORSA,
                                   matcher="dollar_price:city_buy_sell"))
    return out


def DOLLAR_PRICE_table(text: str) -> List[Reading]:
    """Eras B, C, D and F."""
    return scan_city_prices(text, price_pattern=PRICE_DEFAULT, window=34,
                            share_runs=True, layer=LAYER_BORSA,
                            matcher_id="dollar_price:city_table")


def DOLLAR_PRICE_table_truncated(text: str) -> List[Reading]:
    """Era D's truncated tail: 'اربيل 151.70' means 151,700."""
    return scan_city_prices(text, price_pattern=PRICE_TRUNC, window=30,
                            share_runs=True, layer=LAYER_BORSA,
                            matcher_id="dollar_price:city_table_truncated")


def DOLLAR_PRICE_short_city(text: str) -> List[Reading]:
    """Era D's bare form: 'البصرة 153' means 153,000."""
    if len(text) > DOLLAR_PRICE_SHORT_POST:
        return []
    return scan_city_prices(text, price_pattern=PRICE_LOOSE, window=20,
                            share_runs=True, layer=LAYER_BORSA,
                            matcher_id="dollar_price:short_city")


DOLLAR_PRICE_PROFILE = ChannelProfile(
    channel="dollar_price",
    title="سعر الدولار اليوم",
    default_layer=LAYER_BORSA,
    strips=[STRIP_GOLD, STRIP_FOREIGN, STRIP_CBI, STRIP_DATES, STRIP_CLOCK,
            STRIP_PERCENT],
    excludes=[
        ("poll", re.compile(r"Anonymous Poll|\bvoters\b", re.I)),
        # Forwarded marketing is the channel's main non-rate content. The
        # scraper records the forward header separately; this catches the
        # body text of the recurring job and product ads.
        ("job_ad", re.compile(
            r"لنشر\s*فرص\s*العمل|njoomadv|VIP_jobs|فرص\s*عمل|للتوظيف", re.I)),
        ("phone_ad", re.compile(r"\b07\d{9}\b")),
        ("product_ad", re.compile(r"مفقسات|صابون|مكيف|GREE|سامسونج", re.I)),
        ("self_promo", re.compile(r"ادع\s*صديقك|لتعم\s*الفائده")),
        ("campaign", re.compile(r"انقذوا|حملة\s*تبرع")),
    ],
    matchers=[
        Matcher(id="dollar_price:narrative_2017", fn=DOLLAR_PRICE_narrative_2017,
                signature=DOLLAR_PRICE_NARRATIVE_SIG, note="era A, 2017 prose bulletin"),
        Matcher(id="dollar_price:city_buy_sell", fn=DOLLAR_PRICE_city_buy_sell,
                signature=DOLLAR_PRICE_BUY_SELL_SIG, note="era E, per-city بيع/شراء"),
        Matcher(id="dollar_price:city_table", fn=DOLLAR_PRICE_table,
                note="eras B/C/D/F, city + price"),
        Matcher(id="dollar_price:city_table_truncated", fn=DOLLAR_PRICE_table_truncated,
                note="era D, truncated tail '151.70'"),
        Matcher(id="dollar_price:short_city", fn=DOLLAR_PRICE_short_city,
                note="era D, bare 3-digit '153'"),
    ],
)


# ==========================================================================
# CHANNEL REGISTRY
# ==========================================================================

PROFILES = {
    '@dollariraqi'.lstrip('@'): DOLLARIRAQI_PROFILE,
    '@iqborsa'.lstrip('@'): IQBORSA_PROFILE,
    '@dollar_price'.lstrip('@'): DOLLAR_PRICE_PROFILE,
}


def get_profile(channel: str):
    """The format rules for one channel. Raises if the channel is unknown."""
    key = channel.lstrip('@').lower()
    for name, prof in PROFILES.items():
        if name.lower() == key:
            return prof
    raise KeyError(f"No format profile for '{channel}'. "
                   f"Known channels: {', '.join(sorted(PROFILES))}")


def channel_names():
    return sorted(PROFILES)


# ==========================================================================
# POST -> READINGS
# ==========================================================================

@dataclass
class ParseResult:
    readings: List[Reading] = field(default_factory=list)
    matcher: Optional[str] = None
    #: Why nothing was extracted: an exclusion reason code, or 'no_match'.
    dropped: Optional[str] = None
    #: Normalized text the matchers actually saw, for debugging.
    text: str = ""
    #: Readings that WERE extracted correctly but belong to a label the
    #: dataset does not publish -- retail, or a de-scoped city. Kept on the
    #: result and written nowhere. They exist so the boundary behaviour these
    #: labels provide stays testable: the reason retail and the de-scoped
    #: cities are still matched at all is that their labels stop a neighbouring
    #: market from claiming their number, and a test can only assert that if it
    #: can see what the label captured.
    discarded: List[Reading] = field(default_factory=list)

    def __bool__(self):
        return bool(self.readings)

    @property
    def all_readings(self) -> List[Reading]:
        """Everything the matcher extracted, published or not."""
        return self.readings + self.discarded


def parse_post(profile, raw_text: str, forwarded_from: str = "") -> ParseResult:
    """Extract every reading from one post.

    Exclusions are tested against the normalized text WITH urls intact, since
    several channels are identified as adverts by the link they carry. The
    matchers then see a further-cleaned version with the profile's strip
    patterns blanked out, so contaminating figures (gold, foreign currency,
    CBI volumes) cannot be read as rates while the rest of the post is still
    parsed normally.
    """
    body = strip_furniture(raw_text or "")
    with_urls = normalize(body, keep_urls=True)

    if not with_urls.strip():
        return ParseResult(dropped="empty", text="")

    for reason, pattern in profile.excludes:
        if pattern.search(with_urls):
            return ParseResult(dropped=reason, text=with_urls)

    text = normalize(body)
    for strip in profile.strips:
        text = strip.sub(" ", text)

    for matcher in profile.matchers:
        if matcher.signature is not None and not matcher.signature.search(text):
            continue
        readings = matcher.fn(text)
        if readings:
            kept, discarded = _split_discarded(readings)
            # A post that quoted ONLY discarded cities still counts as
            # matched: it was understood, its content is simply not part of
            # this dataset. Recording it as "no_match" would inflate the
            # unmatched file with posts the parser handled correctly.
            return ParseResult(readings=kept, matcher=matcher.id, text=text,
                               discarded=discarded,
                               dropped=None if kept else "discarded_city")

    return ParseResult(dropped="no_match", text=text)


def _split_discarded(readings):
    """Separate publishable readings from labels that only bound the search.

    Retail and the de-scoped regional markets are recognised so that their
    numbers are claimed by their own label and cannot drift into a
    neighbouring city's window (see the note in fx/model.py). Once extraction
    is done they have served their purpose and are held back here -- one
    chokepoint, so no matcher, present or future, can leak an exchange-office
    quote or a de-scoped city into the published series.
    """
    kept, discarded = [], []
    for r in readings:
        (discarded if r.city in DISCARDED_CITIES else kept).append(r)
    return kept, discarded


def parse_posts(channel: str, posts):
    """Yield (post, ParseResult) for every post of one channel."""
    profile = get_profile(channel)
    for post in posts:
        yield post, parse_post(profile, post.get("text", ""),
                               post.get("forwarded_from", ""))


# ==========================================================================
# SCRAPING  —  fetching posts from t.me
# ==========================================================================

#: Telegram timestamps are UTC; Baghdad is a fixed UTC+3 with no DST.
BAGHDAD_TZ = timezone(timedelta(hours=3))

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; iraq-fx-parser/2.0; "
    "+https://github.com/ - research use, contact via repo)"
)

_MSG_ID_RE = re.compile(r"/(\d+)\s*$")


class ScrapeError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# HTML -> post dicts
# ---------------------------------------------------------------------------

def parse_page(html: str, channel: str) -> List[Dict]:
    """Parse one t.me/s/<channel> page into post dicts, ascending by id."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    posts = []
    for msg in soup.select("div.tgme_widget_message"):
        m = _MSG_ID_RE.search(msg.get("data-post", ""))
        if not m:
            continue

        text_el = msg.select_one(".tgme_widget_message_text")
        text = text_el.get_text("\n") if text_el else ""

        time_el = msg.select_one(".tgme_widget_message_date time")
        iso = time_el.get("datetime") if time_el else None
        dt_utc = date_baghdad = time_baghdad = None
        if iso:
            dt = datetime.fromisoformat(iso).astimezone(BAGHDAD_TZ)
            dt_utc = iso
            date_baghdad = dt.strftime("%Y-%m-%d")
            time_baghdad = dt.strftime("%H:%M:%S")

        # Forward provenance matters: some channels' forwarded posts carry real
        # rates, while others' forwarded posts are adverts.
        fwd_el = msg.select_one(".tgme_widget_message_forwarded_from_name")
        forwarded_from = fwd_el.get_text(" ").strip() if fwd_el else ""

        posts.append({
            "channel": channel,
            "message_id": int(m.group(1)),
            "datetime_utc": dt_utc,
            "date_baghdad": date_baghdad,
            "time_baghdad": time_baghdad,
            "text": text,
            "forwarded_from": forwarded_from,
            "permalink": f"https://t.me/{channel}/{m.group(1)}",
            "has_photo": bool(msg.select_one(".tgme_widget_message_photo_wrap")),
        })

    posts.sort(key=lambda p: p["message_id"])
    return posts


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def cache_path(data_dir: Path, channel: str) -> Path:
    return Path(data_dir) / "raw" / f"{channel}.jsonl"


def load_cache(path: Path) -> Dict[int, Dict]:
    if not Path(path).exists():
        return {}
    out = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                p = json.loads(line)
            except json.JSONDecodeError:
                continue  # tolerate a torn final line from an interrupted run
            out[p["message_id"]] = p
    return out


def write_cache(path: Path, posts: Dict[int, Dict]) -> None:
    """Rewrite the cache atomically, sorted by message id."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for mid in sorted(posts):
            f.write(json.dumps(posts[mid], ensure_ascii=False) + "\n")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

class Fetcher:
    """Single-threaded fetcher with backoff and a hard request budget."""

    def __init__(self, sleep=1.5, jitter=0.5, timeout=30, max_retries=4,
                 budget=None, user_agent=DEFAULT_USER_AGENT, log=print):
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept-Language": "ar,en;q=0.8",
        })
        self.sleep = sleep
        self.jitter = jitter
        self.timeout = timeout
        self.max_retries = max_retries
        self.budget = budget
        self.used = 0
        self.log = log

    def _pause(self):
        time.sleep(max(0.0, self.sleep + random.uniform(0, self.jitter)))

    def get(self, url: str) -> Optional[str]:
        if self.budget is not None and self.used >= self.budget:
            raise ScrapeError(f"request budget of {self.budget} exhausted")

        delay = 2.0
        for attempt in range(1, self.max_retries + 1):
            self.used += 1
            try:
                resp = self.session.get(url, timeout=self.timeout)
            except Exception as exc:
                if attempt == self.max_retries:
                    raise ScrapeError(f"{url}: {exc}") from exc
                self.log(f"    network error ({exc}); retry in {delay:.0f}s")
                time.sleep(delay)
                delay *= 2
                continue

            if resp.status_code == 200:
                self._pause()
                return resp.text

            if resp.status_code == 404:
                return None

            if resp.status_code in (429, 500, 502, 503, 504):
                wait = delay
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait = max(wait, float(retry_after))
                    except ValueError:
                        pass
                if attempt == self.max_retries:
                    raise ScrapeError(
                        f"{url}: HTTP {resp.status_code} after "
                        f"{self.max_retries} attempts")
                self.log(f"    HTTP {resp.status_code}; backing off {wait:.0f}s")
                time.sleep(wait)
                delay *= 2
                continue

            raise ScrapeError(f"{url}: HTTP {resp.status_code}")

        return None


# ---------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------

def scrape_channel(channel: str,
                   data_dir: Path,
                   *,
                   mode: str = "auto",
                   since: Optional[str] = None,
                   max_posts: Optional[int] = None,
                   max_pages: Optional[int] = None,
                   fetcher: Optional[Fetcher] = None,
                   log=print) -> Dict:
    """Crawl one channel into its cache. Returns a summary dict."""
    path = cache_path(data_dir, channel)
    cache = load_cache(path)
    before_count = len(cache)
    fetcher = fetcher or Fetcher(log=log)

    base = f"https://t.me/s/{channel}"
    added = 0
    pages = 0
    stop_reason = "completed"

    if mode == "auto":
        mode = "update" if cache else "backfill"

    # An update walks forward from the newest page; a backfill walks back
    # from the oldest id we already hold.
    before = None
    if mode == "backfill" and cache:
        before = min(cache)

    prev_min = None
    log(f"  {channel}: {mode} (cache holds {before_count} posts)")

    while True:
        url = f"{base}?before={before}" if before else base
        try:
            html = fetcher.get(url)
        except ScrapeError as exc:
            stop_reason = f"stopped: {exc}"
            log(f"  ! {stop_reason}")
            break

        if html is None:
            stop_reason = "channel page unavailable"
            break

        page = parse_page(html, channel)
        if not page:
            stop_reason = "empty page (start of history)"
            break

        pages += 1
        fresh = [p for p in page if p["message_id"] not in cache]
        for p in fresh:
            cache[p["message_id"]] = p
        added += len(fresh)

        # Checkpoint every page: an interrupted crawl never loses work.
        write_cache(path, cache)

        min_id = min(p["message_id"] for p in page)
        oldest = min((p["date_baghdad"] for p in page if p["date_baghdad"]),
                     default=None)
        log(f"    page {pages}: {len(page)} posts ({len(fresh)} new), "
            f"down to id {min_id} ({oldest}); cache {len(cache)}")

        if mode == "update" and not fresh:
            stop_reason = "caught up with cache"
            break
        if since and oldest and oldest < since:
            stop_reason = f"reached --since {since}"
            break
        if max_posts and added >= max_posts:
            stop_reason = f"reached --max-posts {max_posts}"
            break
        if max_pages and pages >= max_pages:
            stop_reason = f"reached --max-pages {max_pages}"
            break
        if prev_min is not None and min_id >= prev_min:
            stop_reason = "pagination stopped making progress"
            break

        prev_min = min_id
        before = min_id

    dates = [p["date_baghdad"] for p in cache.values() if p["date_baghdad"]]
    summary = {
        "channel": channel,
        "mode": mode,
        "pages": pages,
        "added": added,
        "total": len(cache),
        "first_date": min(dates) if dates else None,
        "last_date": max(dates) if dates else None,
        "stop_reason": stop_reason,
        "path": str(path),
    }
    log(f"  {channel}: +{added} posts, {len(cache)} cached "
        f"({summary['first_date']} .. {summary['last_date']}) — {stop_reason}")
    return summary


# ==========================================================================
# DAILY TABLE  —  open/close per day, then averaging channels
# ==========================================================================

#: A source is flagged as disagreeing with the others when its daily close
#: for a city differs from the cross-source median by more than this.
OUTLIER_PCT = 0.01  # 1%


def is_weekend(date_str: str, weekend=frozenset({4, 5})) -> bool:
    return datetime.strptime(date_str, "%Y-%m-%d").weekday() in weekend


def _round(x):
    return round_half_up(x, 2)


# ---------------------------------------------------------------------------
# Long-format readings
# ---------------------------------------------------------------------------

READING_FIELDS = ["channel", "date_baghdad", "time_baghdad", "message_id",
                  "city", "layer", "side", "rate", "matcher", "permalink"]


def readings_rows(channel: str, parsed) -> List[Dict]:
    """Flatten (post, ParseResult) pairs into one row per reading."""
    rows = []
    for post, result in parsed:
        for r in result.readings:
            rows.append({
                "channel": channel,
                "date_baghdad": post.get("date_baghdad"),
                "time_baghdad": post.get("time_baghdad"),
                "message_id": post.get("message_id"),
                "city": r.city,
                "layer": r.layer,
                "side": r.side,
                "rate": r.rate,
                "matcher": r.matcher,
                "permalink": post.get("permalink"),
            })
    rows.sort(key=lambda r: (r["date_baghdad"] or "", r["message_id"] or 0))
    return rows


def write_readings(path: Path, rows: List[Dict]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=READING_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# Per-source daily table
# ---------------------------------------------------------------------------

def build_daily(rows: List[Dict], *, layer: str = LAYER_BORSA,
                weekend=frozenset({4, 5})) -> List[Dict]:
    """Open/close per city per day for one source and one market layer.

    A city's open is its rate in the first post naming it that day and its
    close is the rate in the last, ordered by message id.
    """
    by_day = defaultdict(lambda: defaultdict(list))
    bids = defaultdict(lambda: defaultdict(list))

    for r in rows:
        date = r["date_baghdad"]
        if not date or is_weekend(date, weekend):
            continue
        if r["layer"] != layer:
            continue
        bucket = by_day if r["side"] in ASK_SIDES else bids
        bucket[date][r["city"]].append((r["message_id"], r["rate"]))

    out = []
    for date in sorted(set(by_day) | set(bids)):
        cities = by_day.get(date, {})
        row = {"date": date}
        open_close = {}

        for city in SERIES_CITY_KEYS:
            obs = sorted(cities.get(city, []))
            if obs:
                o, c = obs[0][1], obs[-1][1]
                open_close[city] = (o, c)
                row[f"{city}_open"] = o
                row[f"{city}_close"] = c
                row[f"{city}_n"] = len(obs)
            else:
                row[f"{city}_open"] = None
                row[f"{city}_close"] = None
                row[f"{city}_n"] = 0

        bag_open, bag_close, bag_src = pick_baghdad(open_close)
        row["baghdad_open"] = _round(bag_open)
        row["baghdad_close"] = _round(bag_close)
        row["baghdad_market"] = bag_src

        # Bid side, reported but never mixed into the ask series.
        day_bids = [v for obs in bids.get(date, {}).values() for _, v in obs]
        row["bid_min"] = _round(min(day_bids)) if day_bids else None
        row["bid_max"] = _round(max(day_bids)) if day_bids else None
        row["n_bid_readings"] = len(day_bids)

        out.append(row)
    return out


def daily_fieldnames() -> List[str]:
    fields = ["date"]
    for city in SERIES_CITY_KEYS:
        fields += [f"{city}_open", f"{city}_close", f"{city}_n"]
    fields += ["baghdad_open", "baghdad_close", "baghdad_market",
               "bid_min", "bid_max", "n_bid_readings"]
    return fields


def write_daily(path: Path, rows: List[Dict]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=daily_fieldnames(),
                           extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# Cross-source consensus
# ---------------------------------------------------------------------------

CITY_DAY_FIELDS = ["date", "city", "n_sources", "n_used",
                   "consensus_open", "consensus_close",
                   "min_close", "max_close", "spread", "spread_pct",
                   "outlier_sources", "sources"]


def screened_mean(by_channel: Dict[str, float], tol: float = OUTLIER_PCT):
    """Mean across sources, after discarding any source that disagrees with
    the rest by more than `tol`.

    Rationale. On clean data this is identical to a plain average: across the
    whole corpus the two differ by a median of 0.000 IQD. The screen only
    matters when a parser misreads something. Injecting one bad print into a
    day with three sources moves a plain mean by ~22 IQD and this by ~0.1 --
    and that is not hypothetical, it is what a trade quantity misread as a
    price ("100,000" dollars offered at 157,300) did to one day's data.

    With only two sources nothing is screened: there is no way to tell which
    of the two is wrong, so both are kept and the disagreement is reported
    instead. That is what `spread_pct` and `n_sources` are for.
    """
    vals = list(by_channel.values())
    if len(vals) <= 2:
        return statistics.fmean(vals), sorted(by_channel), []

    ref = statistics.median(vals)
    kept = {ch: v for ch, v in by_channel.items()
            if ref and abs(v - ref) / ref <= tol}
    dropped = sorted(set(by_channel) - set(kept))
    if not kept:                      # every source disagreed with the median
        kept, dropped = by_channel, []
    return statistics.fmean(kept.values()), sorted(kept), dropped


def city_day_consensus(per_source_daily: Dict[str, List[Dict]]) -> List[Dict]:
    """One row per (date, city): the cross-source average, plus the
    disagreement measures needed to audit it.

    This is where the extra channels earn their keep. A single source's number
    is unfalsifiable; several sources quoting the same city on the same day
    either agree, which is evidence, or they do not, which is a flag worth
    investigating rather than averaging away.
    """
    closes = defaultdict(dict)   # (date, city) -> {channel: close}
    opens = defaultdict(dict)
    for channel, rows in per_source_daily.items():
        for row in rows:
            for city in SERIES_CITY_KEYS:
                c = row.get(f"{city}_close")
                if c is not None:
                    closes[(row["date"], city)][channel] = c
                o = row.get(f"{city}_open")
                if o is not None:
                    opens[(row["date"], city)][channel] = o

    out = []
    for key in sorted(closes):
        date, city = key
        by_channel = closes[key]
        value, used, dropped = screened_mean(by_channel)
        vals = list(by_channel.values())
        lo, hi = min(vals), max(vals)

        open_by = opens.get(key)
        open_val = screened_mean(open_by)[0] if open_by else None

        out.append({
            "date": date,
            "city": city,
            "n_sources": len(vals),
            "n_used": len(used),
            "consensus_open": _round(open_val),
            "consensus_close": _round(value),
            "min_close": _round(lo),
            "max_close": _round(hi),
            "spread": _round(hi - lo),
            "spread_pct": round((hi - lo) / value * 100, 3) if value else None,
            "outlier_sources": "|".join(dropped),
            "sources": "|".join(f"{ch}:{v:g}"
                                for ch, v in sorted(by_channel.items())),
        })
    return out


CONSENSUS_FIELDS = (["date"]
                    + [f"{c}_consensus" for c in SERIES_CITY_KEYS]
                    + ["baghdad_consensus", "baghdad_market",
                       "n_channels_max", "n_markets_quoted",
                       "max_spread_pct", "flagged_cities"])


def wide_consensus(city_day: List[Dict]) -> List[Dict]:
    """Wide daily table: the consensus close for every market, one row a day."""
    by_date = defaultdict(list)
    for row in city_day:
        by_date[row["date"]].append(row)

    out = []
    for date in sorted(by_date):
        rows = by_date[date]
        row = {"date": date}
        by_city = {r["city"]: r for r in rows}

        for city in SERIES_CITY_KEYS:
            row[f"{city}_consensus"] = (
                by_city[city]["consensus_close"] if city in by_city else None)

        # Baghdad is resolved AFTER the cross-source consensus, not before:
        # each sub-market first gets its own consensus across channels, then
        # one deterministic rule picks which sub-market Baghdad is. Doing it in
        # this order means the sub-market choice never depends on which
        # channels happened to post, only on which markets quoted.
        available = {c: (by_city[c]["consensus_open"],
                         by_city[c]["consensus_close"])
                     for c in BAGHDAD_PRIORITY if c in by_city}
        _, baghdad, bag_src = pick_baghdad(available)
        row["baghdad_consensus"] = _round(baghdad)
        row["baghdad_market"] = bag_src

        quoted = [v for v in
                  [baghdad] + [by_city[c]["consensus_close"]
                               for c in NON_BAGHDAD_MARKETS if c in by_city]
                  if v is not None]

        spreads = [r["spread_pct"] for r in rows if r["spread_pct"] is not None]
        flagged = sorted({r["city"] for r in rows if r["outlier_sources"]})

        row["n_channels_max"] = max(r["n_sources"] for r in rows)
        row["n_markets_quoted"] = len(quoted)
        row["max_spread_pct"] = max(spreads) if spreads else None
        row["flagged_cities"] = "|".join(flagged)
        out.append(row)
    return out


def write_rows(path: Path, rows: List[Dict], fields: List[str]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def write_json(path: Path, obj) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


# ==========================================================================
# PUBLISH  —  writing daily_fx.csv
# ==========================================================================

# The series is written out as TWO files, split at the December 2020
# devaluation:
#
#   validate_fx.csv   2018-01-01 .. 2020-11-30   close prices only.
#                     The stretch used to check this method against the
#                     Central Bank of Iraq's own published market price.
#   daily_fx.csv      2020-12-01 .. present      open and close.
#                     The dataset proper. The CBI stopped publishing its daily
#                     market price on 2023-08-03, so from that date on this is
#                     the only daily series there is.
#
# Splitting them keeps the two jobs apart: one file exists to be checked
# against an external benchmark, the other exists to be used.

SPLIT_DATE = "2020-12-01"          # first day of the published dataset
VALIDATION_START = "2018-01-01"    # first day of the validation file

#: The published market panel, in column order.
PUBLISHED_SERIES = ["baghdad"] + NON_BAGHDAD_MARKETS   # baghdad, basra, erbil


def fieldnames(close_only: bool = False) -> List[str]:
    """Columns of the published file.

    close_only=True gives the validation file: closes, no opens.
    """
    fields = ["date"]
    for series in PUBLISHED_SERIES:
        if close_only:
            fields.append(f"{series}_close")
        else:
            fields += [f"{series}_open", f"{series}_close"]
    fields += ["baghdad_market", "channels_used"]
    return fields


def build_published_rows(city_day: List[Dict]) -> List[Dict]:
    """Pivot the per-(date, market) consensus into the published wide format.

    Every rate here is already an average ACROSS CHANNELS: `city_day_consensus`
    took each market on each day, collected the figure from every channel that
    quoted it, discarded any channel more than 1% from the median (only when
    three or more quoted, since with two there is no way to tell which is
    wrong), and averaged what was left.

    So a row records two different things, and it is worth keeping them
    straight:

      `channels_used`   WHICH TELEGRAM CHANNELS the day's numbers came from.
                        Several channels quoting one market are averaged into
                        a single figure.
      `baghdad_market`  WHICH PHYSICAL MARKET in Baghdad the figure describes.
                        Baghdad has several wholesale sub-markets; the series
                        follows one of them at a time, by fixed priority.

    They are independent. A day can be `channels_used = dollariraqi|dollar_price`
    and `baghdad_market = baghdad_kifah`, meaning: two channels both quoted the
    Kifah borsa, and their two figures were averaged.
    """
    by_date = {}
    for r in city_day:
        by_date.setdefault(r["date"], {})[r["city"]] = r

    rows = []
    for date in sorted(by_date):
        cities = by_date[date]
        row = {"date": date}

        available = {c: (cities[c]["consensus_open"],
                         cities[c]["consensus_close"])
                     for c in BAGHDAD_PRIORITY if c in cities}
        bag_open, bag_close, bag_market = pick_baghdad(available)
        row["baghdad_open"] = _round(bag_open)
        row["baghdad_close"] = _round(bag_close)
        row["baghdad_market"] = bag_market

        for city in NON_BAGHDAD_MARKETS:
            rec = cities.get(city)
            row[f"{city}_open"] = rec["consensus_open"] if rec else None
            row[f"{city}_close"] = rec["consensus_close"] if rec else None

        # Which channels actually stand behind this row. Reported over the
        # markets that appear in the file only: naming a channel that
        # contributed to a market the reader cannot see would describe
        # evidence that is not there.
        #
        # A channel screened out as an outlier is excluded here, so
        # `channels_used` lists only the channels whose figures were actually
        # averaged. Which channel was screened out, and by how much, stays in
        # data/consensus_city_day.csv.
        #
        # The `sources` field looks like "dollariraqi:1500|dollar_price:1501", so
        # the channel name is everything before the colon.
        contributing = [cities[c] for c in BAGHDAD_PRIORITY + NON_BAGHDAD_MARKETS
                        if c in cities]
        used, screened_out = set(), set()
        for rec in contributing:
            for item in rec["sources"].split("|"):
                if item:
                    used.add(item.split(":")[0])
            for ch in rec["outlier_sources"].split("|"):
                if ch:
                    screened_out.add(ch)
        row["channels_used"] = "|".join(sorted(used - screened_out))
        rows.append(row)
    return rows


def write_published(path: Path, rows: List[Dict],
                    close_only: bool = False) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = fieldnames(close_only=close_only)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def split_published(rows: List[Dict]):
    """(validation rows, dataset rows), split at the Dec-2020 devaluation."""
    validation = [r for r in rows
                  if VALIDATION_START <= r["date"] < SPLIT_DATE]
    dataset = [r for r in rows if r["date"] >= SPLIT_DATE]
    return validation, dataset


# ==========================================================================
# COMMAND LINE
# ==========================================================================

DEFAULT_CHANNELS = ["dollariraqi", "iqborsa", "dollar_price"]


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def _channels(args):
    return args.channels or DEFAULT_CHANNELS


# ---------------------------------------------------------------------------

def cmd_channels(args):
    print(f"{'channel':<16} {'layer':<8} {'matchers':<9} title")
    for name in channel_names():
        p = get_profile(name)
        print(f"{p.channel:<16} {p.default_layer:<8} "
              f"{len(p.matchers):<9} {p.title}")
    return 0


def cmd_scrape(args):
    data_dir = Path(args.data_dir)
    fetcher = Fetcher(sleep=args.sleep, jitter=args.jitter,
                      budget=args.budget, log=log)
    summaries = []
    for channel in _channels(args):
        summaries.append(scrape_channel(
            channel, data_dir,
            mode=args.mode, since=args.since,
            max_posts=args.max_posts, max_pages=args.max_pages,
            fetcher=fetcher, log=log))
    write_json(data_dir / "scrape_report.json", summaries)
    log(f"\nTotal requests used: {fetcher.used}")
    return 0


def cmd_parse(args):
    data_dir = Path(args.data_dir)
    per_source_daily = {}
    report = {}

    for channel in _channels(args):
        path = cache_path(data_dir, channel)
        cache = load_cache(path)
        if not cache:
            log(f"  {channel}: no cache at {path} — run scrape first. Skipping.")
            continue

        profile = get_profile(channel)
        posts = [cache[k] for k in sorted(cache)]

        parsed, unmatched = [], []
        matcher_hist, drop_hist = Counter(), Counter()

        for post in posts:
            result = parse_post(profile, post.get("text", ""),
                                post.get("forwarded_from", ""))
            parsed.append((post, result))
            if result.readings:
                matcher_hist[result.matcher] += 1
            else:
                drop_hist[result.dropped or "no_match"] += 1
                if result.dropped == "no_match":
                    unmatched.append({**post, "normalized": result.text})

        rows = readings_rows(channel, parsed)
        write_readings(data_dir / "readings" / f"{channel}.csv", rows)

        borsa = build_daily(rows, layer="borsa",
                                      weekend=profile.weekend)
        if borsa:
            write_daily(data_dir / "daily" / f"{channel}.csv", borsa)
            if profile.in_consensus:
                per_source_daily[channel] = borsa

        up = data_dir / "unmatched" / f"{channel}.jsonl"
        up.parent.mkdir(parents=True, exist_ok=True)
        with open(up, "w", encoding="utf-8") as f:
            for p in unmatched:
                f.write(json.dumps(p, ensure_ascii=False) + "\n")

        matched = sum(matcher_hist.values())
        report[channel] = {
            "posts": len(posts),
            "posts_with_readings": matched,
            "match_rate": round(matched / len(posts), 4) if posts else 0.0,
            "readings": len(rows),
            "borsa_days": len(borsa),
            "by_matcher": dict(matcher_hist.most_common()),
            "dropped": dict(drop_hist.most_common()),
        }
        log(f"  {channel:<14} {len(posts):>6} posts  "
            f"{matched:>6} parsed ({matched/max(1,len(posts)):>5.1%})  "
            f"{len(rows):>6} readings  {len(borsa):>5} borsa days")

    if per_source_daily:
        city_day = city_day_consensus(per_source_daily)
        write_rows(data_dir / "consensus_city_day.csv", city_day,
                             CITY_DAY_FIELDS)
        wide = wide_consensus(city_day)
        write_rows(data_dir / "consensus_daily.csv", wide,
                             CONSENSUS_FIELDS)

        # Two published files, split at the December 2020 devaluation.
        published = build_published_rows(city_day)
        validation, dataset = split_published(published)

        validate_path = data_dir / "validate_fx.csv"
        daily_path = data_dir / "daily_fx.csv"
        write_published(validate_path, validation, close_only=True)
        write_published(daily_path, dataset)

        multi = [r for r in city_day if r["n_sources"] > 1]
        screened = [r for r in city_day if r["outlier_sources"]]
        report["_consensus"] = {
            "sources": sorted(per_source_daily),
            "city_day_rows": len(city_day),
            "corroborated_rows": len(multi),
            "screened_rows": len(screened),
            "validation_days": len(validation),
            "dataset_days": len(dataset),
            "median_spread_pct_when_multi": (
                round(sorted(r["spread_pct"] for r in multi)[len(multi) // 2], 3)
                if multi else None),
        }
        log(f"\n  consensus over {len(per_source_daily)} channels: "
            f"{len(city_day)} market-days, {len(multi)} quoted by >1 "
            f"channel, {len(screened)} with a channel screened out")
        log(f"  validation  {len(validation):>5} days "
            f"({VALIDATION_START} .. {SPLIT_DATE}, closes only) -> {validate_path}")
        log(f"  dataset     {len(dataset):>5} days "
            f"({SPLIT_DATE} onward)                 -> {daily_path}")

    write_json(data_dir / "parse_report.json", report)
    return 0


def cmd_check(args):
    """Health check for the published series. Non-zero exit if anything is off.

    Designed to be the last step of the daily refresh, so a silent failure --
    a channel that stopped posting, a parser that broke on a new format, a day
    where two sources disagree sharply -- surfaces instead of quietly
    degrading the dataset.
    """

    data_dir = Path(args.data_dir)
    problems, notes = [], []
    today = date.today()
    cutoff = (today - timedelta(days=args.stale_days)).isoformat()

    report_path = data_dir / "parse_report.json"
    if not report_path.exists():
        log("No parse_report.json — run parse first.")
        return 1
    report = json.loads(report_path.read_text(encoding="utf-8"))

    # 1. Is every channel still being scraped and parsed?
    for channel in _channels(args):
        dates = [r["date"]
                 for r in _read_csv(data_dir / "daily" / f"{channel}.csv")]
        if not dates:
            problems.append(f"{channel}: no daily rows at all")
            continue
        last = max(dates)
        if last < cutoff:
            problems.append(
                f"{channel}: no data since {last} (older than "
                f"{args.stale_days} days)")
        else:
            notes.append(f"{channel}: current to {last}")

        stats = report.get(channel, {})
        rate = stats.get("match_rate")
        if rate is not None and rate < args.min_match_rate:
            problems.append(
                f"{channel}: match rate {rate:.1%} below "
                f"{args.min_match_rate:.0%} — a format may have changed")

    # 2. Are the sources still agreeing?
    recent = [r for r in _read_csv(data_dir / "consensus_city_day.csv")
              if r["date"] >= cutoff and int(r["n_sources"]) > 1]
    bad = [r for r in recent
           if r["spread_pct"] and float(r["spread_pct"]) > args.max_spread_pct]
    for r in bad:
        problems.append(
            f"{r['date']} {r['city']}: sources disagree by "
            f"{float(r['spread_pct']):.2f}% [{r['sources']}]")
    notes.append(f"{len(recent)} corroborated city-days in the last "
                 f"{args.stale_days} days, {len(bad)} disagreeing")

    for n in notes:
        log(f"  ok    {n}")
    for p in problems:
        log(f"  FLAG  {p}")
    log(f"\n{len(problems)} issue(s)")
    return 1 if problems else 0


def _read_csv(path):
    path = Path(path)
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def cmd_audit(args):
    """Show posts that parsed to nothing, so the profile can be extended."""
    data_dir = Path(args.data_dir)
    path = data_dir / "unmatched" / f"{args.channel}.jsonl"
    if not path.exists():
        log(f"No unmatched file at {path}. Run parse first.")
        return 1
    shown = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            post = json.loads(line)
            print("-" * 70)
            print(f"{post['permalink']}  {post.get('date_baghdad')} "
                  f"{post.get('time_baghdad')}")
            print(post.get("normalized", "")[:600])
            shown += 1
            if shown >= args.limit:
                break
    print("-" * 70)
    print(f"showed {shown} unmatched posts from {path}")
    return 0


# ---------------------------------------------------------------------------

def build_parser():
    ap = argparse.ArgumentParser(
        prog="run_fx.py",
        description="Scrape several Iraqi FX Telegram channels and build "
                    "per-source and consensus USD/IQD daily series.")
    ap.add_argument("--data-dir", default="data")
    sub = ap.add_subparsers(dest="command")

    def add_channels(p):
        p.add_argument("--channels", nargs="*", default=None,
                       help="channel usernames (default: all configured)")

    sub.add_parser("channels", help="list configured channels")

    sp = sub.add_parser("scrape", help="fetch posts into data/raw/<channel>.jsonl")
    add_channels(sp)
    sp.add_argument("--mode", choices=["auto", "update", "backfill"],
                    default="auto",
                    help="auto: update if a cache exists, else backfill")
    sp.add_argument("--since", metavar="YYYY-MM-DD",
                    help="stop backfilling once posts older than this appear")
    sp.add_argument("--max-posts", type=int, default=None,
                    help="stop after adding this many new posts per channel")
    sp.add_argument("--max-pages", type=int, default=None)
    sp.add_argument("--sleep", type=float, default=1.5,
                    help="base delay between requests (default 1.5s)")
    sp.add_argument("--jitter", type=float, default=0.5)
    sp.add_argument("--budget", type=int, default=2000,
                    help="hard cap on total HTTP requests for the whole run")

    pp = sub.add_parser("parse", help="cache -> readings, daily, consensus")
    add_channels(pp)

    rp = sub.add_parser("run", help="scrape then parse")
    add_channels(rp)
    rp.add_argument("--mode", choices=["auto", "update", "backfill"],
                    default="auto")
    rp.add_argument("--since", metavar="YYYY-MM-DD")
    rp.add_argument("--max-posts", type=int, default=None)
    rp.add_argument("--max-pages", type=int, default=None)
    rp.add_argument("--sleep", type=float, default=1.5)
    rp.add_argument("--jitter", type=float, default=0.5)
    rp.add_argument("--budget", type=int, default=2000)

    au = sub.add_parser("audit", help="print posts that parsed to nothing")
    au.add_argument("--channel", required=True)
    au.add_argument("--limit", type=int, default=25)

    ck = sub.add_parser("check", help="health check; non-zero exit on problems")
    add_channels(ck)
    ck.add_argument("--stale-days", type=int, default=5,
                    help="flag a channel with no data in this many days")
    ck.add_argument("--min-match-rate", type=float, default=0.35,
                    help="flag a channel whose parse rate falls below this")
    ck.add_argument("--max-spread-pct", type=float, default=1.0,
                    help="flag recent city-days where sources disagree by more")
    return ap


def main(argv=None):
    args = build_parser().parse_args(argv)
    command = args.command or "run"

    if command == "channels":
        return cmd_channels(args)
    if command == "scrape":
        return cmd_scrape(args)
    if command == "parse":
        return cmd_parse(args)
    if command == "audit":
        return cmd_audit(args)
    if command == "check":
        return cmd_check(args)
    if command == "run":
        rc = cmd_scrape(args)
        return rc or cmd_parse(args)
    build_parser().print_help()
    return 1


# ==========================================================================
# ENTRY POINT
# ==========================================================================

if __name__ == "__main__":
    sys.exit(main())
