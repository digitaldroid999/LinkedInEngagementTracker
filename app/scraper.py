"""Orchestrates LinkedIn API + Google Sheets for engagement scraping."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable

from app.dates import days_ago, parse_datetime
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
        progress_cb: Callable[[str, int, int], None] | None = None,
        stop_check: Callable[[], bool] | None = None,
    ) -> None:
        self._api = api
        self._sheets = sheets
        self._progress_cb = progress_cb
        self._stop_check = stop_check
        self._exp_cache: dict[str, list[tuple[str, str | None]]] = {}
        self._current_label = ""
        self._stats = ScrapeStats()
        self._pending_engagements: list[dict[str, Any]] = []

    def _record_error(self, message: str) -> None:
        self._stats.errors.append(message)
        _log.error("%s", message)

    def _emit(self) -> None:
        if self._progress_cb:
            self._progress_cb(self._current_label, self._stats.new_comments, self._stats.new_reactions)

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
        self._pending_engagements.extend(rows)
        dedup.add(key)
        if engagement_type == "Comment":
            self._stats.new_comments += 1
        else:
            self._stats.new_reactions += 1
        self._emit()

    def _flush_pending_engagements(self) -> None:
        if not self._pending_engagements:
            return
        batch = self._pending_engagements
        self._pending_engagements = []
        self._sheets.append_engagement_dicts(batch)

    def _process_stream(
        self,
        pages,
        engagement_type: str,
        date_fn,
        watermark: datetime | None,
        cutoff: date,
        engager_url: str,
        engager_name: str,
        scrape_date: str,
        dedup: set[tuple[str, str, str]],
    ) -> datetime | None:
        """Process paginated comments or reactions; return first-item datetime from first page."""
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

            stop_after_page = False
            for item in page:
                if self._stop_check and self._stop_check():
                    self._stats.stopped = True
                    stop_after_page = True
                    break
                d = date_fn(item)
                if not d:
                    continue
                if watermark is not None and d <= watermark:
                    stop_after_page = True
                    break
                if d.date() < cutoff:
                    stop_after_page = True
                    break
                post, author = _get_post_and_author(item)
                plink = _post_link(item)
                post_date_s = d.date().isoformat()
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

            if stop_after_page:
                break

            last_d = date_fn(page[-1])
            if not last_d:
                break
            if last_d.date() < cutoff:
                break
            if watermark is not None and last_d <= watermark:
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

    def _process_reaction_stream(
        self,
        pages_list: list[list[dict[str, Any]]],
        date_fn: Callable[[dict[str, Any]], datetime | None],
        cutoff: date,
        engager_url: str,
        engager_name: str,
        scrape_date: str,
        dedup: set[tuple[str, str, str]],
    ) -> None:
        """
        Reactions: a post older than ``cutoff`` does not stop the stream immediately.
        Compare with the next post's timestamp: if the next is strictly older, stop (API order
        is decaying). If the next is newer or equal, keep going (out-of-order / mixed feed).
        If there is no next item, stop.
        """
        for pi, page in enumerate(pages_list):
            if not page:
                continue

            stop_after_page = False
            for i, item in enumerate(page):
                if self._stop_check and self._stop_check():
                    self._stats.stopped = True
                    stop_after_page = True
                    break
                d = date_fn(item)
                if not d:
                    continue
                if d.date() < cutoff:
                    d_next = self._next_engagement_datetime_after(pages_list, pi, i + 1, date_fn)
                    if d_next is None or d_next < d:
                        stop_after_page = True
                        break
                    continue
                post, author = _get_post_and_author(item)
                plink = _post_link(item)
                post_date_s = d.date().isoformat()
                self._append_engagement(
                    engager_url,
                    engager_name,
                    "Reaction",
                    post,
                    author,
                    post_date_s,
                    scrape_date,
                    plink,
                    dedup,
                )

            if stop_after_page:
                break

            last_d = date_fn(page[-1])
            if not last_d:
                break

    def scrape_profile(
        self,
        pinfo: dict[str, Any],
        engager_name: str,
        scrape_date: str,
        dedup: set[tuple[str, str, str]],
        util_sheet_row: int | None = None,
    ) -> None:
        util_updates: list[tuple[str, str]] = []
        self._pending_engagements.clear()
        util_row: int | None = util_sheet_row

        try:
            if self._stop_check and self._stop_check():
                self._stats.stopped = True
                return
            if util_row is None:
                util_row = self._sheets.find_engagement_util_sheet_row(pinfo.get("profile_url") or "")
            username = linkedin_username_from_url(pinfo["profile_url"])
            if not username:
                self._record_error(f"No LinkedIn username in URL for row {pinfo.get('sheet_row')}")
                return
            urn = (pinfo.get("urn") or "").strip()
            try:
                data = _profile_data(self._api, username)
                profile_urn = (data.get("urn") or "").strip()
                profile_name = (data.get("full_name") or "").strip()
                if profile_name:
                    engager_name = profile_name
                if profile_urn:
                    urn = profile_urn
                    if util_row is not None:
                        util_updates.append(("Urn", urn))
            except LinkedInAPIError as e:
                if not urn:
                    self._record_error(f"Profile {username}: {e}")
                    return
            if not urn:
                self._record_error(f"Missing URN for {username}")
                return

            engager_url = pinfo["profile_url"].strip()
            lc = parse_datetime(pinfo.get("last_commented"))
            last_reacted_post_id = (pinfo.get("last_reacted_post_id") or "").strip()
            cutoff = days_ago(90)
            self._exp_cache.clear()

            try:
                fc = self._process_stream(
                    self._api.iter_comment_pages(urn),
                    "Comment",
                    _engagement_date_comment,
                    lc,
                    cutoff,
                    engager_url,
                    engager_name,
                    scrape_date,
                    dedup,
                )
                if fc and util_row is not None:
                    util_updates.append(("Last Commented Date", fc.date().isoformat()))
            except LinkedInAPIError as e:
                self._record_error(f"Comments {username}: {e}")

            if self._stats.stopped:
                return

            try:
                reaction_pages, top_post_id = self._collect_reaction_pages(urn, last_reacted_post_id)
                if reaction_pages:
                    self._process_reaction_stream(
                        reaction_pages,
                        _engagement_date_reaction,
                        cutoff,
                        engager_url,
                        engager_name,
                        scrape_date,
                        dedup,
                    )
                if top_post_id and util_row is not None:
                    util_updates.append(("Last Reacted Post ID", top_post_id))
            except LinkedInAPIError as e:
                self._record_error(f"Reactions {username}: {e}")
        finally:
            self._flush_pending_engagements()
            if util_row is not None and util_updates:
                try:
                    self._sheets.update_engagement_util_cells_batch(util_row, util_updates)
                except Exception as e:
                    _log.warning("Profiles_Engagement_Util batch update failed: %s", e)
            self._emit()

    def run(self) -> ScrapeStats:
        self._stats = ScrapeStats()
        scrape_date = datetime.now().date().isoformat()
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
        _log.info("Starting scrape: %d profile(s) to process.", total)
        if not total:
            return self._stats

        try:
            util_urls, util_row_by_key, util_fields_by_key = self._sheets.load_engagement_util_index()
        except Exception as e:
            _log.warning("Profiles_Engagement_Util sheet unavailable (%s); skipping util registration.", e)
            util_urls = None
            util_row_by_key = None
            util_fields_by_key = None

        for idx, pinfo in enumerate(targets, start=1):
            if self._stop_check and self._stop_check():
                self._stats.stopped = True
                _log.info("Scrape stopped by user after %d/%d profile(s).", idx - 1, total)
                return self._stats
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

            if util_urls is not None and profile_key and profile_key not in util_urls:
                try:
                    new_row = self._sheets.append_profiles_engagement_util_row(
                        self._current_label,
                        pinfo["profile_url"].strip(),
                    )
                    util_urls.add(profile_key)
                    if new_row is not None:
                        if util_row_by_key is not None:
                            util_row_by_key[profile_key] = new_row
                        util_sheet_row = new_row
                except Exception as e:
                    _log.warning("Could not append profile to Profiles_Engagement_Util: %s", e)

            self.scrape_profile(
                pinfo, self._current_label, scrape_date, dedup, util_sheet_row=util_sheet_row
            )
            if self._stats.stopped:
                return self._stats
            _log.info("[Progress] %d/%d profiles scraped.", idx, total)

        _log.info(
            "Scrape run finished. New comments: %d, new reactions: %d, error lines: %d.",
            self._stats.new_comments,
            self._stats.new_reactions,
            len(self._stats.errors),
        )
        return self._stats
