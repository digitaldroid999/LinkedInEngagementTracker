"""Orchestrates LinkedIn API + Google Sheets for engagement scraping."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Iterator, Literal, Protocol

from app.dates import days_ago, format_sheet_date, format_sheet_datetime, parse_datetime
from app.linkedin_api import LinkedInAPIClient, LinkedInAPIError
from app.scrape_log import LOGGER_NAME
from app.sheets import CURRENT_COMPANY_NOTE_KEY, SheetsManager, normalize_profile_url_key

_log = logging.getLogger(f"{LOGGER_NAME}.scraper")


def linkedin_username_from_url(url: str) -> str | None:
    if not url:
        return None
    u = url.strip()
    m = re.search(r"linkedin\.com/in/([^/?#]+)", u, re.I)
    if not m:
        return None
    return m.group(1).strip().rstrip("/")


def _get_post_and_author(item: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    post = item.get("post")
    if not isinstance(post, dict):
        post = {}
    author = post.get("author")
    if not isinstance(author, dict):
        author = {}
    return post, author


def _poster_type(post: dict[str, Any], author: dict[str, Any]) -> str:
    at = (post.get("account_type") or author.get("account_type") or "").strip().lower()
    return "Person" if at == "user" else "Company"


def _engagement_date_comment(item: dict[str, Any]) -> datetime | None:
    d = parse_datetime(item.get("created_at"))
    if d:
        return d
    comment_obj = item.get("comment")
    if isinstance(comment_obj, dict):
        d = parse_datetime(comment_obj.get("created_at"))
        if d:
            return d
    post, _ = _get_post_and_author(item)
    d = parse_datetime(post.get("created_at"))
    if d:
        return d
    activity = item.get("activity")
    if isinstance(activity, dict):
        d = parse_datetime(activity.get("created_at"))
        if d:
            return d
    return None


def _engagement_date_reaction(item: dict[str, Any]) -> datetime | None:
    d = parse_datetime(item.get("created_at"))
    if d:
        return d
    post, _ = _get_post_and_author(item)
    d = parse_datetime(post.get("created_at"))
    if d:
        return d
    activity = item.get("activity")
    if isinstance(activity, dict):
        d = parse_datetime(activity.get("created_at"))
        if d:
            return d
    return None


def _post_link(item: dict[str, Any]) -> str:
    post, _ = _get_post_and_author(item)
    return (item.get("url") or post.get("url") or "").strip()


def _reaction_post_id(item: dict[str, Any]) -> str:
    """Post id from nested ``post`` on a reaction list item (API data array)."""
    post, _ = _get_post_and_author(item)
    pid = post.get("id")
    if pid is None:
        return ""
    return str(pid).strip()


def _poster_name(post: dict[str, Any], author: dict[str, Any]) -> str:
    return (author.get("full_name") or author.get("name") or post.get("full_name") or "").strip()


def _poster_profile_url(author: dict[str, Any], poster_type: str) -> str:
    u = (author.get("url") or author.get("profile_url") or "").strip()
    if u:
        return u
    pid = (author.get("public_identifier") or author.get("username") or "").strip()
    if poster_type == "Person" and pid:
        return f"https://www.linkedin.com/in/{pid}/"
    return ""


def _poster_urn(author: dict[str, Any]) -> str | None:
    for k in ("urn", "entity_urn", "profile_urn"):
        v = author.get(k)
        if v:
            return str(v)
    return None


def _incremental_engagement_scrape(util_sheet_row: int | None, urn_at_start: str) -> bool:
    """
    True when the profile already has a Profiles_Engagement_Util row and a stored URN
    (profile API can be skipped). Per-stream watermarks are applied only when the
    corresponding util columns are non-empty — see ``_comment_reaction_watermarks``.
    """
    if util_sheet_row is None:
        return False
    return bool((urn_at_start or "").strip())


def _comment_reaction_watermarks(
    incremental: bool, pinfo: dict[str, Any]
) -> tuple[datetime | None, str]:
    """
    Comment watermark and reaction bookmark for this run.

    - Not incremental (no util row or no URN): no watermarks — full ~90-day window.
    - Incremental: use ``Last Commented Date`` only when that cell is non-empty and
      parses; otherwise ``None`` (full comment window). Use ``Last Reacted Post ID``
      only when non-empty; otherwise ``""`` (no bookmark — full reaction fetch).
    """
    if not incremental:
        return None, ""
    lc_raw = (pinfo.get("last_commented") or "").strip()
    lc: datetime | None = None
    if lc_raw:
        lc = parse_datetime(pinfo.get("last_commented"))
        if lc is None:
            _log.warning(
                "Unparseable Last Commented Date %r; using full comment window.",
                lc_raw[:120],
            )
    lr = (pinfo.get("last_reacted_post_id") or "").strip()
    return lc, lr


@dataclass(frozen=True)
class EngagementScrapeWindow:
    """
    Rules for how far back to scrape comments and how to stop, for one profile run.

    **Initial comment scrape** (full ~90-day lookback): ``comment_watermark`` is ``None``.
    Items older than ``cutoff`` stop the stream.

    **Incremental comment scrape** (subsequent runs): ``comment_watermark`` is the last
    processed comment time from the sheet. Items at or before that watermark stop the
    stream; items older than ``cutoff`` also stop.

    **Reactions** use the same ``cutoff`` calendar day. When an item is older than
    ``cutoff``, :meth:`reaction_below_cutoff_stops_stream` applies (feed ordering); this
    is used for both initial and incremental reaction processing. Incremental reaction
    *fetch* additionally uses a post-id bookmark in :meth:`_collect_reaction_pages`.
    """

    cutoff: date
    comment_watermark: datetime | None

    def is_initial_comment_scrape(self) -> bool:
        return self.comment_watermark is None

    def comment_item_stops_stream(self, d: datetime) -> bool:
        """If True, stop consuming the comment feed before recording this item."""
        if self.comment_watermark is not None and d <= self.comment_watermark:
            return True
        if d.date() < self.cutoff:
            return True
        return False

    def comment_pagination_stops_after_page(self, last_d: datetime | None) -> bool:
        """If True, do not request further comment pages after finishing the current page."""
        if last_d is None:
            return True
        if last_d.date() < self.cutoff:
            return True
        if self.comment_watermark is not None and last_d <= self.comment_watermark:
            return True
        return False

    @staticmethod
    def reaction_below_cutoff_stops_stream(d: datetime, d_next: datetime | None) -> bool:
        """
        When a reaction is older than ``cutoff`` (calendar day), compare with the next
        item in feed order (newest first). Stop if there is no next item or the next is
        strictly older than ``d`` (decaying feed); otherwise continue (out-of-order).
        """
        if d_next is None or d_next < d:
            return True
        return False


EngagementPagedItemAction = Literal["stop", "skip", "record"]


class PagedEngagementPolicy(Protocol):
    """Comment or reaction rules for :meth:`EngagementScraper._consume_paged_engagement_page`."""

    def item_action(
        self,
        scraper: Any,
        d: datetime,
        item: dict[str, Any],
        *,
        pages_list: list[list[dict[str, Any]]],
        page_idx: int,
        item_idx: int,
        date_fn: Callable[[dict[str, Any]], datetime | None],
    ) -> EngagementPagedItemAction: ...

    def after_page(self, last_d: datetime | None, page_idx: int) -> bool: ...


class CommentPagedPolicy:
    """Per-item and per-page rules for the comment API stream."""

    __slots__ = ("_window",)

    def __init__(self, window: EngagementScrapeWindow) -> None:
        self._window = window

    def item_action(
        self,
        scraper: Any,
        d: datetime,
        item: dict[str, Any],
        *,
        pages_list: list[list[dict[str, Any]]],
        page_idx: int,
        item_idx: int,
        date_fn: Callable[[dict[str, Any]], datetime | None],
    ) -> EngagementPagedItemAction:
        del scraper, item, pages_list, page_idx, item_idx, date_fn
        if self._window.comment_item_stops_stream(d):
            return "stop"
        return "record"

    def after_page(self, last_d: datetime | None, page_idx: int) -> bool:
        del page_idx
        return self._window.comment_pagination_stops_after_page(last_d)


class ReactionBatchPagedPolicy:
    """Per-item rules for reactions when all pages are already loaded (incremental fetch)."""

    __slots__ = ("_window",)

    def __init__(self, window: EngagementScrapeWindow) -> None:
        self._window = window

    def item_action(
        self,
        scraper: Any,
        d: datetime,
        item: dict[str, Any],
        *,
        pages_list: list[list[dict[str, Any]]],
        page_idx: int,
        item_idx: int,
        date_fn: Callable[[dict[str, Any]], datetime | None],
    ) -> EngagementPagedItemAction:
        del item
        if d.date() >= self._window.cutoff:
            return "record"
        d_next = scraper._next_engagement_datetime_after(
            pages_list, page_idx, item_idx + 1, date_fn
        )
        if self._window.reaction_below_cutoff_stops_stream(d, d_next):
            return "stop"
        return "skip"

    def after_page(self, last_d: datetime | None, page_idx: int) -> bool:
        del page_idx
        return last_d is None


class ReactionLazyPagedPolicy:
    """Per-item rules for initial reaction fetch with lazy API pagination."""

    __slots__ = ("_window", "_page_iter", "_first_top_ref")

    def __init__(
        self,
        window: EngagementScrapeWindow,
        page_iter: Iterator[list[dict[str, Any]]],
        first_top_post_id_ref: list[str],
    ) -> None:
        self._window = window
        self._page_iter = page_iter
        self._first_top_ref = first_top_post_id_ref

    def item_action(
        self,
        scraper: Any,
        d: datetime,
        item: dict[str, Any],
        *,
        pages_list: list[list[dict[str, Any]]],
        page_idx: int,
        item_idx: int,
        date_fn: Callable[[dict[str, Any]], datetime | None],
    ) -> EngagementPagedItemAction:
        del item
        if d.date() >= self._window.cutoff:
            return "record"
        d_next = scraper._next_reaction_datetime_after_lazy(
            pages_list,
            self._page_iter,
            page_idx,
            item_idx + 1,
            date_fn,
            self._first_top_ref,
        )
        if self._window.reaction_below_cutoff_stops_stream(d, d_next):
            return "stop"
        return "skip"

    def after_page(self, last_d: datetime | None, page_idx: int) -> bool:
        del page_idx
        return last_d is None


def _profile_data(api: LinkedInAPIClient, username: str) -> dict[str, Any]:
    r = api.get_profile_by_username(username)
    data = r.get("data")
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}


@dataclass
class ScrapeStats:
    new_comments: int = 0
    new_reactions: int = 0
    errors: list[str] = field(default_factory=list)
    stopped: bool = False


class EngagementScraper:
    def __init__(
        self,
        api: LinkedInAPIClient,
        sheets: SheetsManager,
        progress_cb: Callable[[str, int, int, int, int], None] | None = None,
        stop_check: Callable[[], bool] | None = None,
    ) -> None:
        """If ``stop_check`` is set, it is evaluated only between profiles, not during one."""
        self._api = api
        self._sheets = sheets
        self._progress_cb = progress_cb
        self._stop_check = stop_check
        self._exp_cache: dict[str, list[tuple[str, str | None]]] = {}
        self._current_label = ""
        self._stats = ScrapeStats()
        self._pending_engagement_records: list[dict[str, Any]] = []
        self._profile_index = 0
        self._profile_total = 0
        self._api_calls = 0

    def _record_error(self, message: str) -> None:
        self._stats.errors.append(message)
        _log.error("%s", message)

    def _increment_api_calls(self) -> None:
        self._api_calls += 1

    def _emit(self) -> None:
        if self._progress_cb:
            self._progress_cb(
                self._current_label,
                self._stats.new_comments,
                self._stats.new_reactions,
                self._profile_index,
                self._profile_total,
            )

    def _present_companies(self, poster_urn: str) -> list[tuple[str, str | None]]:
        if poster_urn in self._exp_cache:
            return self._exp_cache[poster_urn]
        out: list[tuple[str, str | None]] = []
        try:
            for page in self._api.iter_experience_pages(poster_urn):
                for row in page:
                    dt = row.get("date") or {}
                    end = ""
                    if isinstance(dt, dict):
                        end = str(dt.get("end") or "").strip().lower()
                    if end != "present":
                        continue
                    comp = row.get("company") or {}
                    if not isinstance(comp, dict):
                        continue
                    name = (comp.get("name") or "").strip()
                    url = (comp.get("url") or "").strip() or None
                    if name:
                        out.append((name, url))
        except LinkedInAPIError as e:
            _log.warning("Experience API failed for poster URN %s: %s", poster_urn, e)
        self._exp_cache[poster_urn] = out
        return out

    @staticmethod
    def _dedupe_companies(companies: list[tuple[str, str | None]]) -> list[tuple[str, str | None]]:
        seen: set[str] = set()
        out: list[tuple[str, str | None]] = []
        for name, url in companies:
            nu = (url or "").strip().lower().rstrip("/")
            nn = name.strip().lower()
            key = nu if nu else f"name:{nn}"
            if key in seen:
                continue
            seen.add(key)
            out.append((name, url))
        return out

    @staticmethod
    def _company_urls_note(companies: list[tuple[str, str | None]]) -> str:
        blocks: list[str] = []
        for name, url in companies:
            u = (url or "").strip()
            blocks.append(f"{name}: {u}" if u else f"{name}:")
        return "\n\n".join(blocks)

    def _rows_for_person_poster(
        self,
        kw: dict[str, Any],
        headline: str,
        poster_urn: str | None,
    ) -> list[dict[str, Any]]:
        if not poster_urn:
            return [
                self._sheets.build_engagement_record(
                    **kw, current_company="", company_link=None, profile_headline=headline
                )
            ]
        companies = self._dedupe_companies(self._present_companies(poster_urn))
        if not companies:
            return [
                self._sheets.build_engagement_record(
                    **kw, current_company="", company_link=None, profile_headline=headline
                )
            ]
        if len(companies) == 1:
            n0, u0 = companies[0]
            return [
                self._sheets.build_engagement_record(
                    **kw, current_company=n0, company_link=u0, profile_headline=headline
                )
            ]
        names_joined = ", ".join(n for n, _ in companies)
        rec = self._sheets.build_engagement_record(
            **kw,
            current_company=names_joined,
            company_link=None,
            profile_headline=headline,
        )
        rec[CURRENT_COMPANY_NOTE_KEY] = self._company_urls_note(companies)
        return [rec]

    def _append_engagement(
        self,
        engager_url: str,
        engager_name: str,
        engagement_type: str,
        post: dict[str, Any],
        author: dict[str, Any],
        post_date_s: str,
        scrape_date: str,
        plink: str,
        dedup: set[tuple[str, str, str]],
    ) -> None:
        key = (engager_name.strip(), engagement_type, plink.strip())
        if key in dedup:
            return
        pt = _poster_type(post, author)
        poster_name = _poster_name(post, author)
        poster_link = _poster_profile_url(author, pt)
        headline = (author.get("description") or author.get("headline") or "").strip()
        kw = {
            "engager_link": engager_url,
            "engager_name": engager_name,
            "engagement_type": engagement_type,
            "poster_type": pt,
            "poster_link": poster_link,
            "poster_name": poster_name,
            "engagement_date": post_date_s,
            "post_link": plink,
            "scrape_date": scrape_date,
        }
        if pt == "Person":
            pur = _poster_urn(author)
            rows = self._rows_for_person_poster(kw, headline, pur)
        else:
            rows = [
                self._sheets.build_engagement_record(
                    **kw,
                    current_company="",
                    company_link=None,
                    profile_headline="",
                )
            ]
        self._pending_engagement_records.extend(rows)
        dedup.add(key)
        if engagement_type == "Comment":
            self._stats.new_comments += 1
        else:
            self._stats.new_reactions += 1
        self._emit()

    def _flush_pending_engagements(self) -> None:
        """Write queued Engagements rows in one API batch (per profile)."""
        if not self._pending_engagement_records:
            return
        batch = list(self._pending_engagement_records)
        self._pending_engagement_records.clear()
        try:
            self._sheets.append_engagement_dicts(batch)
        except Exception:
            self._pending_engagement_records.extend(batch)
            raise

    def _consume_paged_engagement_page(
        self,
        pages_list: list[list[dict[str, Any]]],
        page_idx: int,
        page: list[dict[str, Any]],
        date_fn: Callable[[dict[str, Any]], datetime | None],
        policy: PagedEngagementPolicy,
        engagement_type: str,
        engager_url: str,
        engager_name: str,
        scrape_date: str,
        dedup: set[tuple[str, str, str]],
    ) -> bool:
        """
        One page of comments or reactions: apply ``policy`` per item and record when
        action is ``record``. Returns True if the outer stream should stop.
        """
        for item_idx, item in enumerate(page):
            d = date_fn(item)
            if not d:
                continue
            action = policy.item_action(
                self,
                d,
                item,
                pages_list=pages_list,
                page_idx=page_idx,
                item_idx=item_idx,
                date_fn=date_fn,
            )
            if action == "stop":
                return True
            if action == "skip":
                continue
            post, author = _get_post_and_author(item)
            plink = _post_link(item)
            post_date_s = format_sheet_date(d)
            self._append_engagement(
                engager_url,
                engager_name,
                engagement_type,
                post,
                author,
                post_date_s,
                scrape_date,
                plink,
                dedup,
            )
        return False

    def _process_comment_stream(
        self,
        pages,
        engagement_type: str,
        date_fn,
        window: EngagementScrapeWindow,
        engager_url: str,
        engager_name: str,
        scrape_date: str,
        dedup: set[tuple[str, str, str]],
    ) -> datetime | None:
        """Process paginated comments via :class:`CommentPagedPolicy`."""
        policy = CommentPagedPolicy(window)
        pages_list: list[list[dict[str, Any]]] = []
        first_top: datetime | None = None
        for page in pages:
            if not page:
                break
            if first_top is None:
                for it in page:
                    fd = date_fn(it)
                    if fd:
                        first_top = fd
                        break

            pages_list.append(page)
            page_idx = len(pages_list) - 1
            if self._consume_paged_engagement_page(
                pages_list,
                page_idx,
                page,
                date_fn,
                policy,
                engagement_type,
                engager_url,
                engager_name,
                scrape_date,
                dedup,
            ):
                break

            last_d = date_fn(page[-1])
            if policy.after_page(last_d, page_idx):
                break
        return first_top

    @staticmethod
    def _next_engagement_datetime_after(
        pages_list: list[list[dict[str, Any]]],
        page_idx: int,
        item_idx_after: int,
        date_fn: Callable[[dict[str, Any]], datetime | None],
    ) -> datetime | None:
        """First non-missing engagement datetime after (page_idx, item_idx_after)."""
        for pi in range(page_idx, len(pages_list)):
            page = pages_list[pi]
            start = item_idx_after if pi == page_idx else 0
            for j in range(start, len(page)):
                dt = date_fn(page[j])
                if dt is not None:
                    return dt
        return None

    def _collect_reaction_pages(
        self, urn: str, bookmark_post_id: str
    ) -> tuple[list[list[dict[str, Any]]], str]:
        """
        Fetch reaction pages from the API. Stops requesting further pages once ``bookmark_post_id``
        is seen (incremental runs). Returns (pages to process, post id from first item of the
        first non-empty API page — written to the sheet after the run).
        """
        bookmark = bookmark_post_id.strip()
        pages_out: list[list[dict[str, Any]]] = []
        first_top_post_id = ""

        for page in self._api.iter_reaction_pages(urn):
            if not page:
                break
            if not first_top_post_id:
                first_top_post_id = _reaction_post_id(page[0])

            if bookmark:
                cut: int | None = None
                for i, item in enumerate(page):
                    if _reaction_post_id(item) == bookmark:
                        cut = i
                        break
                if cut is not None:
                    if cut > 0:
                        pages_out.append(page[:cut])
                    return pages_out, first_top_post_id

            pages_out.append(list(page))

        return pages_out, first_top_post_id

    @staticmethod
    def _ensure_reaction_pages_after(
        pages_list: list[list[dict[str, Any]]],
        page_iter: Iterator[list[dict[str, Any]]],
        pi: int,
        start_i: int,
    ) -> bool:
        """
        Ensure ``pages_list`` can supply items at ``(pi, start_i)`` onward for
        ``_next_engagement_datetime_after``. If ``start_i`` is past the end of
        ``pages_list[pi]``, perform one API call (``next(page_iter)``) and append
        that page so the last item of the previous page can be compared with the
        first item of the next page before applying cutoff logic.
        """
        if pi >= len(pages_list):
            return False
        page = pages_list[pi]
        if start_i < len(page):
            return True
        if pi + 1 < len(pages_list):
            return True
        nxt = next(page_iter, None)
        if not nxt:
            return False
        pages_list.append(nxt)
        return True

    def _next_reaction_datetime_after_lazy(
        self,
        pages_list: list[list[dict[str, Any]]],
        page_iter: Iterator[list[dict[str, Any]]],
        pi: int,
        start_i: int,
        date_fn: Callable[[dict[str, Any]], datetime | None],
        first_top_post_id_ref: list[str],
    ) -> datetime | None:
        """
        Like ``_next_engagement_datetime_after``, but loads additional API pages when needed
        so items with missing dates on intermediate pages do not hide a later timestamp.
        """
        while True:
            if not self._ensure_reaction_pages_after(pages_list, page_iter, pi, start_i):
                return None
            d_next = self._next_engagement_datetime_after(pages_list, pi, start_i, date_fn)
            if d_next is not None:
                return d_next
            nxt = next(page_iter, None)
            if not nxt:
                return None
            if not first_top_post_id_ref[0]:
                first_top_post_id_ref[0] = _reaction_post_id(nxt[0])
            pages_list.append(nxt)

    def _process_reaction_stream_initial(
        self,
        urn: str,
        date_fn: Callable[[dict[str, Any]], datetime | None],
        window: EngagementScrapeWindow,
        engager_url: str,
        engager_name: str,
        scrape_date: str,
        dedup: set[tuple[str, str, str]],
    ) -> str:
        """
        Initial reaction fetch (no bookmark): fetch pages lazily from the API. When the
        stream needs the item after the last entry on a page, the next page is requested
        first; then ``_next_engagement_datetime_after`` and ``window`` apply the same
        cutoff / ordering rules as :meth:`_process_reaction_stream`.
        """
        page_iter = iter(self._api.iter_reaction_pages(urn))
        pages_list: list[list[dict[str, Any]]] = []
        first_top_ref: list[str] = [""]

        def append_next_page() -> bool:
            page = next(page_iter, None)
            if not page:
                return False
            if not first_top_ref[0]:
                first_top_ref[0] = _reaction_post_id(page[0])
            pages_list.append(page)
            return True

        if not append_next_page():
            return ""

        policy = ReactionLazyPagedPolicy(window, page_iter, first_top_ref)
        pi = 0
        while pi < len(pages_list):
            page = pages_list[pi]
            if not page:
                pi += 1
                if pi >= len(pages_list) and not append_next_page():
                    break
                continue

            if self._consume_paged_engagement_page(
                pages_list,
                pi,
                page,
                date_fn,
                policy,
                "Reaction",
                engager_url,
                engager_name,
                scrape_date,
                dedup,
            ):
                break

            last_d = date_fn(page[-1])
            if policy.after_page(last_d, pi):
                break
            pi += 1
            if pi >= len(pages_list):
                if not append_next_page():
                    break

        return first_top_ref[0]

    def _process_reaction_stream(
        self,
        pages_list: list[list[dict[str, Any]]],
        date_fn: Callable[[dict[str, Any]], datetime | None],
        window: EngagementScrapeWindow,
        engager_url: str,
        engager_name: str,
        scrape_date: str,
        dedup: set[tuple[str, str, str]],
    ) -> None:
        """
        Reactions: a post older than ``window.cutoff`` does not stop the stream immediately.
        Uses :meth:`EngagementScrapeWindow.reaction_below_cutoff_stops_stream` with the next
        post's timestamp (newest-first feed).
        """
        policy = ReactionBatchPagedPolicy(window)
        for page_idx, page in enumerate(pages_list):
            if not page:
                continue

            if self._consume_paged_engagement_page(
                pages_list,
                page_idx,
                page,
                date_fn,
                policy,
                "Reaction",
                engager_url,
                engager_name,
                scrape_date,
                dedup,
            ):
                break

            last_d = date_fn(page[-1])
            if policy.after_page(last_d, page_idx):
                break

    def scrape_profile(
        self,
        pinfo: dict[str, Any],
        engager_name: str,
        scrape_date: str,
        dedup: set[tuple[str, str, str]],
        util_sheet_row: int | None = None,
    ) -> None:
        self._api_calls = 0
        self._api.set_api_call_hook(self._increment_api_calls)
        util_row_holder: list[int | None] = [util_sheet_row]
        try:
            self._scrape_profile_body(
                pinfo, engager_name, scrape_date, dedup, util_row_holder
            )
        finally:
            self._api.set_api_call_hook(None)
            ur = util_row_holder[0]
            if ur is not None:
                try:
                    self._sheets.update_engagement_util_cell(
                        ur, "Number of API calls", str(self._api_calls)
                    )
                except Exception as e:
                    _log.warning("Could not write Number of API calls for row %s: %s", ur, e)

    def _scrape_profile_body(
        self,
        pinfo: dict[str, Any],
        engager_name: str,
        scrape_date: str,
        dedup: set[tuple[str, str, str]],
        util_row_holder: list[int | None],
    ) -> None:
        util_row = util_row_holder[0]
        if util_row is None:
            util_row = self._sheets.find_engagement_util_sheet_row(pinfo.get("profile_url") or "")
        util_row_holder[0] = util_row

        urn_at_start = (pinfo.get("urn") or "").strip()
        incremental = _incremental_engagement_scrape(util_row, urn_at_start)
        label = linkedin_username_from_url(pinfo["profile_url"]) or pinfo["profile_url"]
        if incremental:
            _log.info("Engagement scrape: util row + URN present (incremental-capable) for %s.", label)
        else:
            _log.info(
                "Engagement scrape: full window (~90 days, no util watermarks) for %s.",
                label,
            )

        username = linkedin_username_from_url(pinfo["profile_url"])
        if not username:
            self._record_error(f"No LinkedIn username in URL for row {pinfo.get('sheet_row')}")
            return
        urn = urn_at_start
        # Existing util row with URN: skip profile API; use sheet URN for comments/reactions.
        skip_profile_api = util_row is not None and bool(urn)
        if skip_profile_api:
            _log.info(
                "Skipping profile API for %s (Urn already set on Profiles_Engagement_Util).",
                username,
            )
        else:
            try:
                data = _profile_data(self._api, username)
                profile_urn = (data.get("urn") or "").strip()
                profile_name = (data.get("full_name") or "").strip()
                if profile_name:
                    engager_name = profile_name
                if profile_urn:
                    urn = profile_urn
                if util_row is not None:
                    if urn:
                        self._sheets.update_engagement_util_cell(util_row, "Urn", urn)
                elif urn:
                    new_row = self._sheets.append_profiles_engagement_util_row(
                        engager_name,
                        pinfo["profile_url"].strip(),
                        urn=urn,
                    )
                    if new_row is not None:
                        util_row = new_row
                        util_row_holder[0] = util_row
            except LinkedInAPIError as e:
                if not urn:
                    self._record_error(f"Profile {username}: {e}")
                    return
        if not urn:
            self._record_error(f"Missing URN for {username}")
            return

        engager_url = pinfo["profile_url"].strip()
        lc, last_reacted_post_id = _comment_reaction_watermarks(incremental, pinfo)
        if incremental:
            full_comments = lc is None
            full_reactions = not last_reacted_post_id
            if full_comments and full_reactions:
                _log.info(
                    "Last Commented Date and Last Reacted Post ID empty; "
                    "scraping full ~90-day comments and reactions for %s.",
                    label,
                )
            elif full_comments:
                _log.info(
                    "Last Commented Date empty; full comment window for %s (reactions use bookmark if set).",
                    label,
                )
            elif full_reactions:
                _log.info(
                    "Last Reacted Post ID empty; full reaction fetch for %s (comments use watermark if set).",
                    label,
                )
        window = EngagementScrapeWindow(cutoff=days_ago(90), comment_watermark=lc)
        self._exp_cache.clear()

        try:
            try:
                fc = self._process_comment_stream(
                    self._api.iter_comment_pages(urn),
                    "Comment",
                    _engagement_date_comment,
                    window,
                    engager_url,
                    engager_name,
                    scrape_date,
                    dedup,
                )
                if fc and util_row is not None:
                    self._sheets.update_engagement_util_cell(
                        util_row, "Last Commented Date", format_sheet_datetime(fc)
                    )
            except LinkedInAPIError as e:
                self._record_error(f"Comments {username}: {e}")

            try:
                if last_reacted_post_id.strip():
                    reaction_pages, top_post_id = self._collect_reaction_pages(urn, last_reacted_post_id)
                    if reaction_pages:
                        self._process_reaction_stream(
                            reaction_pages,
                            _engagement_date_reaction,
                            window,
                            engager_url,
                            engager_name,
                            scrape_date,
                            dedup,
                        )
                else:
                    top_post_id = self._process_reaction_stream_initial(
                        urn,
                        _engagement_date_reaction,
                        window,
                        engager_url,
                        engager_name,
                        scrape_date,
                        dedup,
                    )
                if top_post_id and util_row is not None:
                    self._sheets.update_engagement_util_cell(
                        util_row, "Last Reacted Post ID", top_post_id
                    )
            except LinkedInAPIError as e:
                self._record_error(f"Reactions {username}: {e}")
        finally:
            self._flush_pending_engagements()

    def run(self) -> ScrapeStats:
        self._stats = ScrapeStats()
        scrape_date = format_sheet_date(datetime.now())
        headers, rows = self._sheets.load_profiles_table()
        if not headers:
            _log.warning("Profiles sheet has no headers; nothing to scrape.")
            return self._stats
        dedup = self._sheets.load_engagement_dedup_keys()

        targets: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            pinfo = self._sheets.find_profile_row(headers, i, row)
            if not pinfo.get("profile_url") or "linkedin.com/in/" not in pinfo["profile_url"].lower():
                if pinfo.get("profile_url"):
                    _log.info(
                        "[Skip] Row %s: URL is not a LinkedIn /in/ profile link.",
                        pinfo.get("sheet_row"),
                    )
                continue
            targets.append(pinfo)

        total = len(targets)
        self._profile_total = total
        _log.info("Starting scrape: %d profile(s) to process.", total)
        if not total:
            return self._stats

        try:
            _, util_row_by_key, util_fields_by_key = self._sheets.load_engagement_util_index()
        except Exception as e:
            _log.warning("Profiles_Engagement_Util sheet unavailable (%s); skipping util registration.", e)
            util_row_by_key = None
            util_fields_by_key = None

        for idx, pinfo in enumerate(targets, start=1):
            self._profile_index = idx
            self._current_label = (
                pinfo.get("display_name") or linkedin_username_from_url(pinfo["profile_url"]) or "Profile"
            )
            self._emit()
            profile_key = normalize_profile_url_key(pinfo.get("profile_url") or "")
            if util_fields_by_key is not None and profile_key and profile_key in util_fields_by_key:
                pinfo.update(util_fields_by_key[profile_key])

            util_sheet_row: int | None = None
            if util_row_by_key is not None and profile_key:
                util_sheet_row = util_row_by_key.get(profile_key)

            self.scrape_profile(
                pinfo, self._current_label, scrape_date, dedup, util_sheet_row=util_sheet_row
            )
            # Stop is honored only between profiles so the current profile always finishes.
            if self._stop_check and self._stop_check():
                self._stats.stopped = True
                _log.info(
                    "Scrape stopped by user after completing profile %d/%d.",
                    idx,
                    total,
                )
                return self._stats
            _log.info("[Progress] %d/%d profiles scraped.", idx, total)

        _log.info(
            "Scrape run finished. New comments: %d, new reactions: %d, error lines: %d.",
            self._stats.new_comments,
            self._stats.new_reactions,
            len(self._stats.errors),
        )
        return self._stats
