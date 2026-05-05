from __future__ import annotations

import re
from typing import Any, Dict, Optional


PARK_FACTOR_SOURCE = "static_park_factor_v1"


def normalize_venue_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    text = str(name).lower().strip()
    text = text.replace("&", "and")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _profile(
    venue_name: str,
    run_factor: float,
    home_run_factor: float,
    hit_factor: float,
    venue_type: str = "outdoor",
    default_roof_status: str = "open",
    weather_applies_default: bool | str = True,
    aliases: Optional[list[str]] = None,
    home_run_factor_lhb: Optional[float] = None,
    home_run_factor_rhb: Optional[float] = None,
) -> Dict[str, Any]:
    normalized = normalize_venue_name(venue_name)
    return {
        "venue_name": venue_name,
        "normalized_venue_name": normalized,
        "aliases": aliases or [],
        "run_factor": run_factor,
        "home_run_factor": home_run_factor,
        "home_run_factor_lhb": home_run_factor_lhb,
        "home_run_factor_rhb": home_run_factor_rhb,
        "hit_factor": hit_factor,
        "venue_type": venue_type,
        "default_roof_status": default_roof_status,
        "weather_applies_default": weather_applies_default,
        "source": PARK_FACTOR_SOURCE,
        "park_factor_profile_found": True,
        "neutral_park_fallback_used": False,
    }


# Conservative first-pass factors centered near 1.000.
# These are intentionally shrinked; they should be refined only after backtesting.
PARK_FACTORS: Dict[str, Dict[str, Any]] = {}

_RAW_PROFILES = [
    _profile("Angel Stadium", 0.99, 0.98, 1.00, aliases=["Angel Stadium of Anaheim"], home_run_factor_lhb=0.98, home_run_factor_rhb=0.98),
    _profile("Busch Stadium", 0.98, 0.96, 0.99, home_run_factor_lhb=0.96, home_run_factor_rhb=0.96),
    _profile("Chase Field", 1.01, 1.02, 1.01, venue_type="retractable", default_roof_status="unknown", weather_applies_default="unknown", home_run_factor_lhb=1.02, home_run_factor_rhb=1.02),
    _profile("Citi Field", 0.98, 0.96, 0.99, home_run_factor_lhb=0.96, home_run_factor_rhb=0.96),
    _profile("Citizens Bank Park", 1.02, 1.08, 1.00, home_run_factor_lhb=1.09, home_run_factor_rhb=1.07),
    _profile("Comerica Park", 1.00, 0.95, 1.02, home_run_factor_lhb=0.95, home_run_factor_rhb=0.95),
    _profile("Coors Field", 1.10, 1.08, 1.06, home_run_factor_lhb=1.08, home_run_factor_rhb=1.08),
    _profile("Daikin Park", 1.00, 1.02, 0.99, venue_type="retractable", default_roof_status="unknown", weather_applies_default="unknown", aliases=["Minute Maid Park"], home_run_factor_lhb=1.03, home_run_factor_rhb=1.01),
    _profile("Dodger Stadium", 0.99, 1.00, 0.99, home_run_factor_lhb=1.00, home_run_factor_rhb=1.00),
    _profile("Fenway Park", 1.03, 0.98, 1.04, home_run_factor_lhb=0.97, home_run_factor_rhb=0.99),
    _profile("Globe Life Field", 0.99, 0.98, 0.99, venue_type="retractable", default_roof_status="unknown", weather_applies_default="unknown", home_run_factor_lhb=0.98, home_run_factor_rhb=0.98),
    _profile("Great American Ball Park", 1.03, 1.10, 0.99, home_run_factor_lhb=1.10, home_run_factor_rhb=1.10),
    _profile("Guaranteed Rate Field", 1.01, 1.05, 0.99, aliases=["Rate Field"], home_run_factor_lhb=1.05, home_run_factor_rhb=1.05),
    _profile("Kauffman Stadium", 1.01, 0.95, 1.03, home_run_factor_lhb=0.95, home_run_factor_rhb=0.95),
    _profile("loanDepot park", 0.98, 0.96, 0.99, venue_type="retractable", default_roof_status="unknown", weather_applies_default="unknown", aliases=["loanDepot Park", "Marlins Park"], home_run_factor_lhb=0.96, home_run_factor_rhb=0.96),
    _profile("Nationals Park", 1.00, 1.01, 1.00, home_run_factor_lhb=1.01, home_run_factor_rhb=1.01),
    _profile("Oracle Park", 0.97, 0.91, 0.99, home_run_factor_lhb=0.91, home_run_factor_rhb=0.91),
    _profile("Oriole Park at Camden Yards", 1.01, 1.04, 1.00, aliases=["Camden Yards"], home_run_factor_lhb=1.05, home_run_factor_rhb=1.03),
    _profile("PNC Park", 0.99, 0.97, 1.00, home_run_factor_lhb=0.97, home_run_factor_rhb=0.97),
    _profile("Petco Park", 0.97, 0.94, 0.99, home_run_factor_lhb=0.94, home_run_factor_rhb=0.94),
    _profile("Progressive Field", 0.99, 0.98, 1.00, home_run_factor_lhb=0.98, home_run_factor_rhb=0.98),
    _profile("Rogers Centre", 1.01, 1.04, 1.00, venue_type="retractable", default_roof_status="unknown", weather_applies_default="unknown", home_run_factor_lhb=1.04, home_run_factor_rhb=1.04),
    _profile("T-Mobile Park", 0.98, 0.96, 0.99, venue_type="retractable", default_roof_status="unknown", weather_applies_default="unknown", home_run_factor_lhb=0.96, home_run_factor_rhb=0.96),
    _profile("Target Field", 0.99, 1.00, 0.99, home_run_factor_lhb=1.00, home_run_factor_rhb=1.00),
    _profile("Truist Park", 1.01, 1.03, 1.00, home_run_factor_lhb=1.03, home_run_factor_rhb=1.03),
    _profile("Tropicana Field", 0.98, 0.96, 0.99, venue_type="dome", default_roof_status="dome", weather_applies_default=False, home_run_factor_lhb=0.96, home_run_factor_rhb=0.96),
    _profile("Wrigley Field", 1.02, 1.05, 1.00, home_run_factor_lhb=1.05, home_run_factor_rhb=1.05),
    _profile("Yankee Stadium", 1.01, 1.07, 0.99, home_run_factor_lhb=1.09, home_run_factor_rhb=1.05),
    _profile("American Family Field", 1.00, 1.03, 0.99, venue_type="retractable", default_roof_status="unknown", weather_applies_default="unknown", aliases=["Miller Park"], home_run_factor_lhb=1.03, home_run_factor_rhb=1.03),
    _profile("Sutter Health Park", 1.00, 1.00, 1.00, aliases=["Oakland Coliseum", "RingCentral Coliseum"], home_run_factor_lhb=1.00, home_run_factor_rhb=1.00),
]


def _register(profile: Dict[str, Any]) -> None:
    names = [profile["venue_name"], *(profile.get("aliases") or [])]
    for name in names:
        key = normalize_venue_name(name)
        if key:
            PARK_FACTORS[key] = profile


for _row in _RAW_PROFILES:
    _register(_row)


def neutral_park_factor_profile(venue_name: Optional[str]) -> Dict[str, Any]:
    normalized = normalize_venue_name(venue_name)
    return {
        "venue_name": venue_name,
        "normalized_venue_name": normalized,
        "aliases": [],
        "run_factor": 1.0,
        "home_run_factor": 1.0,
        "home_run_factor_lhb": None,
        "home_run_factor_rhb": None,
        "hit_factor": 1.0,
        "venue_type": "unknown",
        "default_roof_status": "unknown",
        "weather_applies_default": "unknown",
        "source": "neutral_fallback_unmapped_venue",
        "park_factor_profile_found": False,
        "neutral_park_fallback_used": True,
    }


def get_park_factor_profile(venue_name: Optional[str]) -> Dict[str, Any]:
    key = normalize_venue_name(venue_name)
    if not key:
        return neutral_park_factor_profile(venue_name)

    profile = PARK_FACTORS.get(key)
    if profile:
        return dict(profile)

    return neutral_park_factor_profile(venue_name)


__all__ = [
    "PARK_FACTOR_SOURCE",
    "normalize_venue_name",
    "get_park_factor_profile",
    "neutral_park_factor_profile",
]
