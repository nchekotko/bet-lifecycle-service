"""Entity matching: decide whether a provider fixture is an event we already know.

Providers spell teams differently ("Manchester United FC", "MAN UTD") and disagree on
kickoff by a few minutes. We normalise names (case, accents, punctuation, club
suffixes, aliases), then require both teams to be similar and kickoffs to be close.
The decision is persisted in ``provider_event_map`` so matching runs once per
provider event.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from difflib import SequenceMatcher

_NOISE = frozenset({"fc", "cf", "afc", "sc", "ac", "club", "the"})
ALIASES: dict[str, str] = {
    "man utd": "manchester united",
    "man united": "manchester united",
    "man city": "manchester city",
    "spurs": "tottenham hotspur",
    "tottenham": "tottenham hotspur",
    "wolves": "wolverhampton wanderers",
    "inter": "internazionale",
    "inter milan": "internazionale",
    "psg": "paris saint germain",
    "bayern": "bayern munchen",
    "bayern munich": "bayern munchen",
}


def normalize_team(name: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    tokens = re.sub(r"[^a-z0-9 ]+", " ", ascii_name.lower()).split()
    cleaned = " ".join(t for t in tokens if t not in _NOISE)
    return ALIASES.get(cleaned, cleaned)


@dataclass(frozen=True, slots=True)
class Candidate:
    event_id: str
    home_norm: str
    away_norm: str
    kickoff: datetime


def similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def best_match(
    home: str,
    away: str,
    kickoff: datetime,
    candidates: Iterable[Candidate],
    *,
    min_similarity: float = 0.85,
    max_kickoff_delta: timedelta = timedelta(minutes=90),
) -> str | None:
    """Return the canonical event id or ``None`` when nothing is close enough."""
    h, a = normalize_team(home), normalize_team(away)
    best: tuple[float, str] | None = None
    for c in candidates:
        if abs(c.kickoff - kickoff) > max_kickoff_delta:
            continue
        score = min(similarity(h, c.home_norm), similarity(a, c.away_norm))
        if score >= min_similarity and (best is None or score > best[0]):
            best = (score, c.event_id)
    return best[1] if best else None
