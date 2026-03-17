"""
Data Cleaner & Transformer
===========================
Input : raw dict produced by VNPTScraper (or loaded from MinIO raw/)
Output: cleaned dict with same keys, ready for Neo4j ingestion

Cleaning steps:
  1. Strip HTML tags left over in text fields
  2. Normalise Unicode (NFC) and fix common Vietnamese encoding artefacts
  3. Collapse multiple whitespace / blank lines
  4. Remove boilerplate / noise lines (copyright notices, navigational text)
  5. Deduplicate entries by id
  6. Validate required fields; mark incomplete records
  7. Chunk long answer content (>1 500 chars) into numbered sub-answers
"""

import html
import logging
import re
import unicodedata
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

try:
    import markdownify as _markdownify  # pip install markdownify
    _HAS_MARKDOWNIFY = True
except ImportError:
    _HAS_MARKDOWNIFY = False

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Noise patterns – lines matching any of these are stripped from answers
# ---------------------------------------------------------------------------
_NOISE_PATTERNS: List[re.Pattern] = [
    re.compile(r"^(copyright|©|\bAll rights reserved\b)", re.I),
    re.compile(r"^\s*[<>]{2,}\s*$"),            # lone arrows
    re.compile(r"^(Trang chủ|Liên hệ|Về chúng tôi|Đăng nhập|Đăng ký)$", re.I),
    re.compile(r"^(Home|Contact|About|Login|Register|Menu)$", re.I),
    re.compile(r"^\s*\d{1,2}/\d{1,2}/\d{4}\s*$"),   # bare dates
    re.compile(r"^https?://\S+$"),                     # bare URLs
    re.compile(r"^[\-_=]{3,}$"),                       # divider lines
]

# Max chars before we chunk an answer
_CHUNK_LIMIT = 1500

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def clean(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Full cleaning pipeline.

    Args:
        raw: dict from VNPTScraper.run()

    Returns:
        cleaned dict with same top-level keys plus 'cleaning_stats'
    """
    result = deepcopy(raw)

    groups   = _clean_groups(result.get("groups",   []))
    topics   = _clean_topics(result.get("topics",   []))
    problems = _clean_problems(result.get("problems", []))

    # Build explicit answer→problem reverse map from declared relations so
    # _clean_answers never has to guess from ID naming conventions.
    ans_to_prob: Dict[str, str] = {
        rel["answer_id"]: rel["problem_id"]
        for rel in result.get("rels_has_answer", [])
        if "answer_id" in rel and "problem_id" in rel
    }
    answers, extra_rels = _clean_answers(result.get("answers", []), ans_to_prob)

    # Deduplicate by id
    groups   = _dedup(groups,   "id")
    topics   = _dedup(topics,   "id")
    problems = _dedup(problems, "id")
    answers  = _dedup(answers,  "id")

    # Merge extra rels from chunking
    rels_ha: List[Dict] = result.get("rels_has_answer", []) + extra_rels

    result.update({
        "groups":          groups,
        "topics":          topics,
        "problems":        problems,
        "answers":         answers,
        "rels_has_topic":  result.get("rels_has_topic",   []),
        "rels_has_problem": result.get("rels_has_problem", []),
        "rels_has_answer": rels_ha,
    })

    stats = {
        "groups":   len(groups),
        "topics":   len(topics),
        "problems": len(problems),
        "answers":  len(answers),
        "rels_has_topic":   len(result["rels_has_topic"]),
        "rels_has_problem": len(result["rels_has_problem"]),
        "rels_has_answer":  len(rels_ha),
    }
    result["cleaning_stats"] = stats
    logger.info("Cleaning complete: %s", stats)
    return result


# ---------------------------------------------------------------------------
# Per-entity cleaners
# ---------------------------------------------------------------------------

def _clean_groups(rows: List[Dict]) -> List[Dict]:
    out = []
    for r in rows:
        r = dict(r)
        r["name"]        = _clean_text(r.get("name", ""))
        r["description"] = _clean_text(r.get("description", ""))
        if not r.get("id") or not r.get("name"):
            continue
        out.append(r)
    return out


def _clean_topics(rows: List[Dict]) -> List[Dict]:
    out = []
    for r in rows:
        r = dict(r)
        r["name"]     = _clean_text(r.get("name", ""))
        r["keywords"] = _clean_text(r.get("keywords", ""))
        if not r.get("id") or not r.get("name"):
            continue
        out.append(r)
    return out


def _clean_problems(rows: List[Dict]) -> List[Dict]:
    out = []
    for r in rows:
        r = dict(r)
        r["title"]            = _clean_text(r.get("title", ""))
        r["description"]      = _clean_content(r.get("description", ""))
        r["keywords"]         = _clean_text(r.get("keywords", ""))
        r["sample_questions"] = _clean_text(r.get("sample_questions", ""))
        r["intent"]           = r.get("intent", "") or ""
        if not r.get("id") or not r.get("title"):
            continue
        out.append(r)
    return out


def _clean_answers(
    rows: List[Dict],
    ans_to_prob: Optional[Dict[str, str]] = None,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Return (cleaned_answers, extra_rels_has_answer).
    Long answers are chunked; extra_rels maps one problem to every child chunk.

    Args:
        rows:        raw answer dicts from the scraper
        ans_to_prob: explicit answer_id → problem_id mapping built from
                     rels_has_answer.  Used instead of ID-name inference so
                     that changing the scraper's ID scheme never silently
                     creates orphaned Answer nodes.
    """
    if ans_to_prob is None:
        ans_to_prob = {}
    out: List[Dict] = []
    extra_rels: List[Dict] = []

    for r in rows:
        r = dict(r)
        r["summary"] = _clean_text(r.get("summary", ""))
        r["content"] = _clean_content(r.get("content", ""))
        r["steps"]   = _clean_content(r.get("steps",   ""))
        r["notes"]   = _clean_content(r.get("notes",   ""))

        if not r.get("id") or not r.get("content"):
            # try summary as content
            if r.get("summary"):
                r["content"] = r["summary"]
            else:
                continue

        # Chunking
        content = r["content"]
        if len(content) <= _CHUNK_LIMIT:
            out.append(r)
            continue

        # Split into chunks
        chunks = _chunk_text(content, _CHUNK_LIMIT)
        # First chunk keeps original id
        r["content"] = chunks[0]
        out.append(r)

        parent_id  = r["id"]
        # Prefer the explicit map; fall back to regex inference only as last resort
        problem_id = ans_to_prob.get(parent_id) or _infer_problem_id(parent_id)

        # Parent node acts as a structural anchor (no long content).
        # This preserves the existing rels_has_answer edge (problem → parent).
        parent_node = dict(r)
        parent_node["content"]    = ""    # full text lives in child chunks
        parent_node["has_chunks"] = True
        out.append(parent_node)

        for i, chunk in enumerate(chunks, start=1):
            child_id = f"{parent_id}__chunk_{i}"
            out.append({
                "id":               child_id,
                "parent_answer_id": parent_id,
                "summary":          f"{r['summary']} (P.{i}/{len(chunks)})",
                "content":          chunk,
                "steps":            r.get("steps",      ""),
                "notes":            r.get("notes",      ""),
                "status":           r.get("status",     "active"),
                "source_url":       r.get("source_url", ""),
                "crawled_at":       r.get("crawled_at", ""),
            })
            if problem_id:
                extra_rels.append({"problem_id": problem_id, "answer_id": child_id})

    return out, extra_rels


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------

def _html_to_markdown(text: str) -> str:
    """
    Convert HTML to Markdown to preserve semantic structure (lists, bold,
    headings) for downstream LLM processing.  Falls back to plain-text
    stripping when markdownify is not installed.
    """
    if not text:
        return ""
    text = html.unescape(text)
    if _HAS_MARKDOWNIFY:
        # strip=True removes unsupported tags while keeping their text content
        return _markdownify.markdownify(text, heading_style="ATX", strip=["a", "img"])
    # ---- fallback: map structural tags to Markdown equivalents ----
    # Convert list items and paragraphs before stripping remaining tags
    text = re.sub(r"<li[^>]*>", "\n- ", text, flags=re.I)
    text = re.sub(r"</li>", "", text, flags=re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</?p[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"<h[1-6][^>]*>(.*?)</h[1-6]>", r"\n## \1\n", text, flags=re.I | re.S)
    text = re.sub(r"<(strong|b)[^>]*>(.*?)</(strong|b)>", r"**\2**", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", "", text)   # strip remaining tags
    return text


def _normalize_unicode(text: str) -> str:
    """NFC normalisation + fix common double-encoded variants."""
    text = unicodedata.normalize("NFC", text)
    # Fix Windows-1252 → UTF-8 artefacts sometimes seen in Vietnamese crawls
    _replacements = {
        "\u00e2\u0080\u0099": "\u2019",  # â€™ → '
        "\u00e2\u0080\u009c": "\u201c",  # â€œ → "
        "\u00e2\u0080\u009d": "\u201d",  # â€ → "
    }
    for bad, good in _replacements.items():
        text = text.replace(bad, good)
    return text


def _collapse_whitespace(text: str) -> str:
    """Convert tabs/multiple spaces to single space; collapse 3+ newlines."""
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _remove_noise_lines(text: str) -> str:
    lines = text.splitlines()
    clean_lines = [ln for ln in lines if not any(p.search(ln) for p in _NOISE_PATTERNS)]
    return "\n".join(clean_lines)


def _clean_text(text: str) -> str:
    """Light cleaning for short fields (name, title, summary, keywords)."""
    if not text:
        return ""
    text = _html_to_markdown(text)
    text = _normalize_unicode(text)
    text = re.sub(r"[ \t]+", " ", text).strip()
    return text


def _clean_content(text: str) -> str:
    """Full cleaning for answer content, steps, notes."""
    if not text:
        return ""
    text = _html_to_markdown(text)
    text = _normalize_unicode(text)
    text = _collapse_whitespace(text)
    text = _remove_noise_lines(text)
    return text


# How far back from the hard limit we allow a word-boundary search (chars)
_SPLIT_LOOKBACK = 80
# Overlap kept between consecutive chunks to preserve cross-boundary context
_CHUNK_OVERLAP  = 100


def _chunk_text(text: str, limit: int) -> List[str]:
    """
    Split *text* into chunks of at most *limit* chars.

    Improvements over naive hard-split:
    - Prefers paragraph > sentence > word boundaries; never cuts mid-word.
    - Adds a small overlap (_CHUNK_OVERLAP) between chunks so that sentences
      spanning a boundary are present in both adjacent chunks, preserving
      context for vector-search retrieval.
    """
    if len(text) <= limit:
        return [text]

    # ---- Phase 1: semantic split at paragraph / sentence boundaries --------
    semantic_chunks: List[str] = []
    paragraphs = re.split(r"\n{2,}", text)
    current = ""
    for para in paragraphs:
        if len(current) + len(para) + 2 <= limit:
            current = (current + "\n\n" + para).lstrip()
        else:
            if current:
                semantic_chunks.append(current.strip())
            if len(para) > limit:
                # Split oversized paragraph at sentence boundaries
                sentences = re.split(r"(?<=[.!?。])\s+", para)
                current = ""
                for sent in sentences:
                    if len(current) + len(sent) + 1 <= limit:
                        current = (current + " " + sent).lstrip()
                    else:
                        if current:
                            semantic_chunks.append(current.strip())
                        current = sent
            else:
                current = para
    if current:
        semantic_chunks.append(current.strip())

    # ---- Phase 2: word-boundary hard-split for any still-oversized chunk ---
    # Also injects overlap so adjacent chunks share _CHUNK_OVERLAP chars.
    final: List[str] = []
    for chunk in semantic_chunks:
        if len(chunk) <= limit:
            final.append(chunk)
            continue
        start = 0
        while start < len(chunk):
            end = start + limit
            if end >= len(chunk):
                final.append(chunk[start:].strip())
                break
            # Walk back to the nearest whitespace to avoid splitting a word
            boundary = chunk.rfind(" ", start, end)
            if boundary == -1 or (end - boundary) > _SPLIT_LOOKBACK:
                boundary = end   # no good boundary found in window – hard cut
            final.append(chunk[start:boundary].strip())
            # Start next chunk _CHUNK_OVERLAP chars before boundary so context
            # sentences are duplicated across the chunk seam.
            start = max(start + 1, boundary - _CHUNK_OVERLAP)

    return final or [text[:limit]]


def _dedup(rows: List[Dict], key: str) -> List[Dict]:
    seen = set()
    out  = []
    for r in rows:
        k = r.get(key)
        if k and k not in seen:
            seen.add(k)
            out.append(r)
    return out


def _infer_problem_id(answer_id: str) -> str:
    """
    FALLBACK ONLY – infer a problem_id from an answer_id using the scraper's
    default naming convention (``__ans_N`` → ``__prob_N``).

    WARNING: This is intentionally a last-resort.  If the scraper changes its
    ID scheme (e.g. switches to hashes), this function silently returns a
    wrong ID and creates orphaned Answer nodes in Neo4j.  Always prefer the
    explicit ``ans_to_prob`` map built from ``rels_has_answer`` in ``clean()``.
    """
    return re.sub(r"__ans_(\d+)$", r"__prob_\1", answer_id)
