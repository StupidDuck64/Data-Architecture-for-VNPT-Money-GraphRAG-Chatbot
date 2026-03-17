"""
VNPT Money Help Page Scraper
Supports two modes:
  - requests + BeautifulSoup (fast, for static HTML)
  - Playwright (for JavaScript-rendered pages)

Target: https://vnptpay.vn/web/trogiup
Output schema matches external_data CSVs:
  nodes_group, nodes_topic, nodes_problem, nodes_answer,
  rels_has_topic, rels_has_problem, rels_has_answer
"""

import re
import json
import time
import logging
import hashlib
import unicodedata
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes that mirror the CSV schema
# ---------------------------------------------------------------------------

@dataclass
class NodeGroup:
    id: str
    name: str
    description: str = ""
    order: int = 0

@dataclass
class NodeTopic:
    id: str
    name: str
    group_id: str
    keywords: str = ""
    order: int = 0

@dataclass
class NodeProblem:
    id: str
    title: str
    description: str = ""
    intent: str = ""
    keywords: str = ""
    sample_questions: str = ""
    status: str = "active"
    source_url: str = ""
    crawled_at: str = ""

@dataclass
class NodeAnswer:
    id: str
    summary: str
    content: str
    steps: str = ""
    notes: str = ""
    status: str = "active"
    source_url: str = ""
    crawled_at: str = ""

@dataclass
class RelHasTopic:
    group_id: str
    topic_id: str

@dataclass
class RelHasProblem:
    topic_id: str
    problem_id: str

@dataclass
class RelHasAnswer:
    problem_id: str
    answer_id: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ACCENTED = (
    "àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ"
    "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞŸ"
    "áàảãạăắặằẵẩẫậâấầẩẫậéèẻẽẹêếềểễệíìỉĩị"
    "óòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ"
)

def slugify(text: str) -> str:
    """Convert Vietnamese text to ASCII slug."""
    # Handle Vietnamese đ/Đ which NFD decomposition cannot strip
    text = text.replace("đ", "d").replace("Đ", "D")
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s_]", "", text)
    text = re.sub(r"[\s]+", "_", text.strip())
    text = re.sub(r"_+", "_", text)
    return text[:80]


def make_id(*parts: str) -> str:
    """Join slugified parts with double underscore."""
    return "__".join(slugify(p) for p in parts if p)


def short_hash(text: str, length: int = 6) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:length]


# ---------------------------------------------------------------------------
# Core scraper
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

BASE_URL = "https://vnptpay.vn/web/trogiup"


class VNPTScraper:
    """
    Scrapes https://vnptpay.vn/web/trogiup and returns structured data
    ready to be loaded into Neo4j.

    Strategy:
      1. Try requests + BeautifulSoup first (fast path).
      2. If the page body appears to be rendered by JS (no FAQ items found),
         fall back to Playwright.
    """

    def __init__(
        self,
        url: str = BASE_URL,
        timeout: int = 30,
        delay: float = 1.5,
        use_playwright: bool = False,
    ):
        self.url = url
        self.timeout = timeout
        self.delay = delay
        self.use_playwright = use_playwright
        self.raw_html: Optional[str] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        """
        Returns a dict with keys:
          groups, topics, problems, answers,
          rels_has_topic, rels_has_problem, rels_has_answer,
          crawled_at, source_url
        """
        logger.info("Starting crawl: %s", self.url)
        html = self._fetch_html()
        if not html:
            raise RuntimeError("Failed to fetch page HTML")

        self.raw_html = html
        result = self._parse(html)
        crawled_at = datetime.now(timezone.utc).isoformat()
        result["crawled_at"] = crawled_at
        result["source_url"] = self.url
        # Inject atomic metadata into every answer and problem so each
        # RAG chunk can be traced back to its exact source.
        for ans in result["answers"]:
            ans["source_url"] = self.url
            ans["crawled_at"] = crawled_at
        for prob in result["problems"]:
            prob["source_url"] = self.url
            prob["crawled_at"] = crawled_at
        logger.info(
            "Crawl complete – %d groups, %d topics, %d problems, %d answers",
            len(result["groups"]),
            len(result["topics"]),
            len(result["problems"]),
            len(result["answers"]),
        )
        return result

    def get_raw_html(self) -> Optional[str]:
        return self.raw_html

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _fetch_html(self) -> Optional[str]:
        if self.use_playwright:
            return self._fetch_playwright()
        html = self._fetch_requests()
        if html and self._looks_static(html):
            return html
        logger.warning("Static fetch seems empty – falling back to Playwright")
        return self._fetch_playwright()

    def _fetch_requests(self) -> Optional[str]:
        try:
            resp = requests.get(self.url, headers=HEADERS, timeout=self.timeout)
            resp.encoding = "utf-8"
            if resp.status_code == 200:
                logger.info("requests: HTTP 200 (%.1f KB)", len(resp.text) / 1024)
                return resp.text
            logger.error("requests: HTTP %d", resp.status_code)
        except Exception as exc:
            logger.error("requests error: %s", exc)
        return None

    def _fetch_playwright(self) -> Optional[str]:
        try:
            from playwright.sync_api import sync_playwright  # type: ignore

            with sync_playwright() as pw:
                # Use browser as context manager to guarantee cleanup on any exception
                with pw.chromium.launch(headless=True) as browser:
                    context = browser.new_context(extra_http_headers=HEADERS)
                    page = context.new_page()
                    try:
                        page.goto(self.url, timeout=self.timeout * 1000, wait_until="networkidle")
                        # Click every visible "Xem thêm" button – there can be
                        # multiple buttons (one per category) appearing simultaneously.
                        # After each sweep, re-count in case lazy-loading revealed new
                        # buttons.  force=True handles buttons that are partially
                        # obscured by sticky headers / overlays.
                        buttons = page.get_by_text("Xem thêm")
                        count = buttons.count()
                        while count > 0:
                            for i in range(count):
                                try:
                                    buttons.nth(i).click(timeout=2000, force=True)
                                    page.wait_for_timeout(500)
                                except Exception:
                                    continue
                            page.wait_for_load_state("networkidle", timeout=5000)
                            new_count = buttons.count()
                            if new_count >= count:
                                # No new buttons appeared – all content loaded
                                break
                            count = new_count
                        # Scroll to bottom to trigger any remaining lazy-loaded content
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        time.sleep(self.delay)
                        html = page.content()
                    finally:
                        context.close()
                    logger.info("playwright: fetched %.1f KB", len(html) / 1024)
                    return html
        except ImportError:
            logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
        except Exception as exc:
            logger.error("Playwright error: %s", exc)
        return None

    @staticmethod
    def _looks_static(html: str) -> bool:
        """
        Return True only if the HTML contains at least 3 FAQ-like content
        elements.  A bare keyword match is not enough because modern pages
        include words like "faq" or "trợ giúp" in their static header/footer
        long before the JS-rendered FAQ list appears.
        """
        soup = BeautifulSoup(html, "html.parser")
        FAQ_SELECTORS = [
            "div.faq-question", "div.accordion-header", "button.accordion-button",
            "div.question-title", "h4.question", "dt",
            "div.faq-item", "div.accordion-item", "div.help-topic",
            "div.help-category", "div.tab-pane", "section.help-section",
        ]
        for sel in FAQ_SELECTORS:
            if len(soup.select(sel)) >= 3:
                return True
        return False

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def _parse(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")

        groups:   List[Dict] = []
        topics:   List[Dict] = []
        problems: List[Dict] = []
        answers:  List[Dict] = []
        rels_ht:  List[Dict] = []
        rels_hp:  List[Dict] = []
        rels_ha:  List[Dict] = []

        # Try multiple selector strategies in order
        parsed = (
            self._parse_strategy_accordion(soup, groups, topics, problems, answers, rels_ht, rels_hp, rels_ha)
            or self._parse_strategy_generic(soup, groups, topics, problems, answers, rels_ht, rels_hp, rels_ha)
        )

        if not parsed or not problems:
            logger.warning("No structured content found – falling back to plain-text extraction")
            self._parse_fallback(soup, groups, topics, problems, answers, rels_ht, rels_hp, rels_ha)

        return {
            "groups":          groups,
            "topics":          topics,
            "problems":        problems,
            "answers":         answers,
            "rels_has_topic":  rels_ht,
            "rels_has_problem": rels_hp,
            "rels_has_answer": rels_ha,
        }

    # ------------------------------------------------------------------
    # Strategy 1: VNPTPay accordion/tab layout (inspect-based selectors)
    # ------------------------------------------------------------------

    def _parse_strategy_accordion(
        self, soup: BeautifulSoup,
        groups, topics, problems, answers, rels_ht, rels_hp, rels_ha,
    ) -> bool:
        """
        Typical structure on vnptpay.vn/web/trogiup:
          <div class="help-category|tab-pane|...">
            <h2|h3 class="category-title">GROUP NAME</h2>
            <div class="topic|section">
              <h3|h4>TOPIC NAME</h4>
              <div class="faq-item|accordion-item|...">
                <div class="question|accordion-header">QUESTION</div>
                <div class="answer|accordion-body|collapse">ANSWER</div>
              </div>
            </div>
          </div>

        We use multiple CSS selector candidates and pick whichever yields data.
        """

        # ---- candidate selectors -------------------------------------------
        GROUP_SELECTORS   = ["div.help-category", "div.tab-pane", "section.help-section", "div.faq-group"]
        TOPIC_SELECTORS   = ["div.help-topic", "div.topic-section", "div.faq-section", "div.help-section"]
        QUESTION_SELECTORS = [
            "div.faq-question", "div.accordion-header", "button.accordion-button",
            "div.question-title", "h4.question", "dt",
        ]
        ANSWER_SELECTORS  = [
            "div.faq-answer", "div.accordion-body", "div.answer-content",
            "div.collapse", "dd", "div.answer",
        ]

        # ---- try to find group containers ----------------------------------
        group_els: List[Tag] = []
        for sel in GROUP_SELECTORS:
            group_els = soup.select(sel)
            if group_els:
                break

        if not group_els:
            # Try tab navigation titles as groups
            group_els = soup.select("li.tab-item, nav.help-nav li, ul.category-list li")

        if not group_els:
            return False

        group_order = 0
        for g_el in group_els:
            group_order += 1
            g_name_el = g_el.find(["h1", "h2", "h3", "span"], class_=re.compile(r"title|name|header", re.I))
            g_name = (g_name_el.get_text(strip=True) if g_name_el else g_el.get_text(separator=" ", strip=True)[:60])
            if not g_name:
                continue

            g_id = make_id(g_name)
            groups.append(asdict(NodeGroup(id=g_id, name=g_name, order=group_order)))

            # ---- find topics inside group ----------------------------------
            topic_els: List[Tag] = []
            for sel in TOPIC_SELECTORS:
                topic_els = g_el.select(sel)
                if topic_els:
                    break

            if not topic_els:
                # Use the group itself as single topic
                topic_els = [g_el]

            topic_order = 0
            for t_el in topic_els:
                topic_order += 1
                t_name_el = t_el.find(["h3", "h4", "h5", "span"], class_=re.compile(r"topic|title|section", re.I))
                t_name = (t_name_el.get_text(strip=True) if t_name_el else g_name)
                t_id = make_id(g_id, t_name)

                # Avoid duplicate topics
                if not any(t["id"] == t_id for t in topics):
                    topics.append(asdict(NodeTopic(id=t_id, name=t_name, group_id=g_id, order=topic_order)))
                    rels_ht.append({"group_id": g_id, "topic_id": t_id})

                # ---- find FAQ items inside topic ---------------------------
                q_els: List[Tag] = []
                for sel in QUESTION_SELECTORS:
                    q_els = t_el.select(sel)
                    if q_els:
                        break

                if not q_els:
                    # Fallback: pair all <p> as Q/A
                    paras = t_el.find_all("p")
                    q_els = paras[::2]

                prob_order = 0
                for q_el in q_els:
                    prob_order += 1
                    q_text = q_el.get_text(separator=" ", strip=True)
                    if len(q_text) < 5:
                        continue

                    # Try to find paired answer
                    a_text = ""
                    a_el: Optional[Tag] = None
                    for sel in ANSWER_SELECTORS:
                        # Use CSS selector on parent container – avoids brittle split(".")[-1]
                        container = q_el.parent or q_el
                        candidates = container.select(sel)
                        # Pick first candidate that is not the question element itself
                        for c in candidates:
                            if c is not q_el:
                                a_el = c
                                break
                        if a_el:
                            break
                    if not a_el and q_el.parent:
                        # Last-resort: parent's next sibling
                        ns = q_el.parent.find_next_sibling()
                        if ns and ns.name not in ["h2", "h3", "h4", "h5"]:
                            a_el = ns

                    if a_el:
                        a_text = a_el.get_text(separator="\n", strip=True)

                    p_id = f"{t_id}__prob_{prob_order}"
                    a_id = f"{t_id}__ans_{prob_order}"
                    intent = slugify(q_text)[:60]

                    problems.append(asdict(NodeProblem(
                        id=p_id, title=q_text[:200], intent=intent,
                    )))
                    answers.append(asdict(NodeAnswer(
                        id=a_id, summary=q_text[:120], content=a_text,
                    )))
                    rels_hp.append({"topic_id": t_id, "problem_id": p_id})
                    rels_ha.append({"problem_id": p_id, "answer_id": a_id})

        return bool(problems)

    # ------------------------------------------------------------------
    # Strategy 2: Generic heading + paragraph pairs
    # ------------------------------------------------------------------

    def _parse_strategy_generic(
        self, soup: BeautifulSoup,
        groups, topics, problems, answers, rels_ht, rels_hp, rels_ha,
    ) -> bool:
        """
        Walk through all headings and treat them as topic sections.
        Content following each heading is treated as Q&A.
        """
        g_id = "ho_tro_khach_hang"
        groups.append(asdict(NodeGroup(id=g_id, name="Hỗ trợ khách hàng", description="Hướng dẫn sử dụng dịch vụ VNPT Money", order=1)))

        topic_order = 0
        prob_order_global = 0

        for heading in soup.find_all(["h2", "h3"]):
            heading_text = heading.get_text(strip=True)
            if len(heading_text) < 3:
                continue

            topic_order += 1
            t_id = make_id(g_id, heading_text)
            if not any(t["id"] == t_id for t in topics):
                topics.append(asdict(NodeTopic(id=t_id, name=heading_text, group_id=g_id, order=topic_order)))
                rels_ht.append({"group_id": g_id, "topic_id": t_id})

            # Everything until next same-level heading
            sibling = heading.find_next_sibling()
            q_text = ""
            a_lines = []
            while sibling and sibling.name not in ["h2", "h3"]:
                text = sibling.get_text(separator=" ", strip=True)
                if sibling.name in ["h4", "h5", "strong", "b"] and text:
                    if q_text:
                        # Save previous Q&A
                        prob_order_global += 1
                        _save_qa(
                            q_text, "\n".join(a_lines), t_id,
                            prob_order_global, problems, answers, rels_hp, rels_ha,
                        )
                    q_text = text
                    a_lines = []
                elif text:
                    a_lines.append(text)
                sibling = sibling.find_next_sibling()

            if q_text:
                prob_order_global += 1
                _save_qa(
                    q_text, "\n".join(a_lines), t_id,
                    prob_order_global, problems, answers, rels_hp, rels_ha,
                )

        return bool(problems)

    # ------------------------------------------------------------------
    # Strategy 3: Fallback – dump all visible text as single FAQ block
    # ------------------------------------------------------------------

    def _parse_fallback(
        self, soup: BeautifulSoup,
        groups, topics, problems, answers, rels_ht, rels_hp, rels_ha,
    ):
        logger.warning("Using fallback text extraction")
        body = soup.find("body") or soup
        text = body.get_text(separator="\n", strip=True)

        g_id = "ho_tro_khach_hang"
        t_id = "ho_tro_khach_hang__tro_giup_khac"

        if not any(g["id"] == g_id for g in groups):
            groups.append(asdict(NodeGroup(id=g_id, name="Hỗ trợ khách hàng", order=1)))
        if not any(t["id"] == t_id for t in topics):
            topics.append(asdict(NodeTopic(id=t_id, name="Trợ giúp khác", group_id=g_id, order=1)))
            rels_ht.append({"group_id": g_id, "topic_id": t_id})

        prob_id = f"{t_id}__prob_1"
        ans_id  = f"{t_id}__ans_1"
        problems.append(asdict(NodeProblem(
            id=prob_id, title="Nội dung trợ giúp VNPT Money", intent="noi_dung_tro_giup",
        )))
        answers.append(asdict(NodeAnswer(
            id=ans_id, summary="Nội dung trợ giúp VNPT Money", content=text[:5000],
        )))
        rels_hp.append({"topic_id": t_id, "problem_id": prob_id})
        rels_ha.append({"problem_id": prob_id, "answer_id": ans_id})


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _save_qa(q_text, a_text, t_id, idx, problems, answers, rels_hp, rels_ha):
    p_id = f"{t_id}__prob_{idx}"
    a_id = f"{t_id}__ans_{idx}"
    # Instantiate dataclasses to validate schema at insertion time
    problems.append(asdict(NodeProblem(
        id=p_id, title=q_text[:200], intent=slugify(q_text)[:60],
    )))
    answers.append(asdict(NodeAnswer(
        id=a_id, summary=q_text[:120], content=a_text,
    )))
    rels_hp.append({"topic_id": t_id, "problem_id": p_id})
    rels_ha.append({"problem_id": p_id, "answer_id": a_id})


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    use_pw = "--playwright" in sys.argv
    scraper = VNPTScraper(use_playwright=use_pw)
    data = scraper.run()

    out = "vnpt_raw_scraped.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved to {out}")
    print(f"  groups: {len(data['groups'])}")
    print(f"  topics: {len(data['topics'])}")
    print(f"  problems: {len(data['problems'])}")
    print(f"  answers: {len(data['answers'])}")
