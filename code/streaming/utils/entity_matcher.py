import re
from pathlib import Path
from typing import List, Dict

import yaml

ROOT_DIR = Path(__file__).resolve().parents[2]  # project root
ENTITIES_PATH = ROOT_DIR / "config" / "entities.yml"


def load_figures() -> List[Dict]:
    with ENTITIES_PATH.open() as f:
        data = yaml.safe_load(f)
    figures = data.get("figures", []) or []
    # normalize patterns to lowercase
    for fobj in figures:
        fobj["patterns"] = [p.lower() for p in fobj.get("patterns", [])]
    return figures


def match_figures_in_text(text: str, figures: List[Dict]) -> List[str]:
    """
    Given tweet text and figure definitions, return list of canonical_names
    that appear in the text based on simple lowercase substring match.
    """
    if not text:
        return []
    t = text.lower()
    matched = []
    for fobj in figures:
        cname = fobj["canonical_name"]
        for pat in fobj.get("patterns", []):
            if pat in t:
                matched.append(cname)
                break  # one pattern is enough
    # deduplicate while preserving order
    seen = set()
    unique = []
    for name in matched:
        if name not in seen:
            seen.add(name)
            unique.append(name)
    return unique