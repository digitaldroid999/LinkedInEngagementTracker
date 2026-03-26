"""Google Sheets access: profiles, engagements, hyperlinks."""

from __future__ import annotations

import re
from typing import Any
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials

from app.config import (
    GOOGLE_SCOPES,
    GOOGLE_SHEET_ID,
    SHEET_ENGAGEMENTS,
    SHEET_PROFILES,
    SHEET_PROFILES_ENGAGEMENT_UTIL,
)

# Internal metadata on engagement dicts; not a sheet column.
CURRENT_COMPANY_NOTE_KEY = "__current_company_note__"


def _norm_header(h: str) -> str:
    return (h or "").strip().lower()


def _hyperlink_formula(url: str, label: str) -> str:
    u = (url or "").replace('"', '""')
    lab = (label or "").replace('"', '""')
    return f'=HYPERLINK("{u}","{lab}")'


def normalize_profile_url_key(url: str) -> str:
    """Stable key for comparing LinkedIn /in/ profile URLs across sheets."""
    s = (url or "").strip()
    if not s:
        return ""
    extracted = _extract_linkedin_url(s)
    u = extracted or s
    m = re.search(r"https?://(?:www\.)?linkedin\.com/in/[^/?#\s]+", u, re.I)
    if m:
        return m.group(0).strip().rstrip("/").lower()
    return u.lower().rstrip("/")


def _extract_linkedin_url(cell: str) -> str:
    """Extract LinkedIn profile URL from plain text or HYPERLINK formula."""
    s = (cell or "").strip()
    if not s:
        return ""
    m = re.search(r"https?://(?:www\.)?linkedin\.com/in/[^\"')\s]+", s, re.I)
    if m:
        return m.group(0).strip()
    return ""


def _column_letter_index(n: int) -> str:
    """0-based column index to A1 letter(s)."""
    s = ""
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


class SheetsManager:
    def __init__(self, service_account_info: dict[str, Any]) -> None:
        credentials = Credentials.from_service_account_info(service_account_info, scopes=GOOGLE_SCOPES)
        gc = gspread.authorize(credentials)
        self._sheet = gc.open_by_key(GOOGLE_SHEET_ID)
        self._profiles: gspread.Worksheet | None = None
        self._engagements: gspread.Worksheet | None = None
        self._profiles_engagement_util: gspread.Worksheet | None = None

    def profile_ws(self) -> gspread.Worksheet:
        if self._profiles is None:
            self._profiles = self._sheet.worksheet(SHEET_PROFILES)
        return self._profiles

    def engagements_ws(self) -> gspread.Worksheet:
        if self._engagements is None:
            self._engagements = self._sheet.worksheet(SHEET_ENGAGEMENTS)
        return self._engagements

    def profiles_engagement_util_ws(self) -> gspread.Worksheet:
        if self._profiles_engagement_util is None:
            self._profiles_engagement_util = self._sheet.worksheet(SHEET_PROFILES_ENGAGEMENT_UTIL)
        return self._profiles_engagement_util

    def load_profile_headers(self) -> list[str]:
        ws = self.profile_ws()
        return ws.row_values(1)

    def load_engagement_headers(self) -> list[str]:
        ws = self.engagements_ws()
        return ws.row_values(1)

    def load_profiles_table(self) -> tuple[list[str], list[list[str]]]:
        ws = self.profile_ws()
        rows = ws.get_all_values(value_render_option="FORMULA")
        if not rows:
            return [], []
        headers = rows[0]
        return headers, rows[1:]

    def load_engagement_dedup_keys(self) -> set[tuple[str, str, str]]:
        ws = self.engagements_ws()
        rows = ws.get_all_values()
        if len(rows) < 2:
            return set()
        headers = [_norm_header(h) for h in rows[0]]
        try:
            i_eng = headers.index("engager name")
            i_type = headers.index("engagement type")
            i_link = headers.index("post link")
        except ValueError:
            return set()
        keys: set[tuple[str, str, str]] = set()
        for r in rows[1:]:
            if len(r) <= max(i_eng, i_type, i_link):
                continue
            keys.add((r[i_eng].strip(), r[i_type].strip(), r[i_link].strip()))
        return keys

    def _header_index_map(self, headers: list[str]) -> dict[str, int]:
        return {_norm_header(h): i for i, h in enumerate(headers)}

    def find_profile_row(
        self,
        headers: list[str],
        row_idx: int,
        row: list[str],
    ) -> dict[str, Any]:
        """Map header names to row data for one profile row."""
        m = self._header_index_map(headers)

        def excel_serial_to_iso(serial_number: float | int) -> str:
            """Serial date/time from Sheets → ISO datetime (preserves fractional-day time)."""
            excel_epoch = datetime(1899, 12, 30)
            try:
                sn = float(serial_number)
            except (ValueError, TypeError):
                return str(serial_number)
            converted = excel_epoch + timedelta(days=sn)
            return converted.isoformat(timespec="seconds")

        def col(*names: str) -> str:
            for n in names:
                k = _norm_header(n)
                if k in m and m[k] < len(row):
                    raw = row[m[k]]
                    if isinstance(raw, (int, float)):
                        return excel_serial_to_iso(raw)
                    elif isinstance(raw, str):
                        s = raw.strip()
                        # print(s)
                        # if s and re.match(r"^-?\d+(\.\d+)?$", s):
                        #     print(2)
                        #     return excel_serial_to_iso(float(s))
                        return s
                    return str(raw).strip()
            return ""

        url = col("linkedin profile")
        url = _extract_linkedin_url(url) or url
        if not url:
            for cell in row:
                extracted = _extract_linkedin_url(cell)
                if extracted:
                    url = extracted
                    break
        return {
            "sheet_row": row_idx + 2,
            "profile_url": url,
            "display_name": col("name"),
            # URN / comment / reaction bookmarks live on Profiles_Engagement_Util, not Profiles.
            "urn": "",
            "last_commented": "",
            "last_reacted_post_id": "",
        }

    def append_engagement_rows(self, rows: list[list[Any]]) -> dict[str, Any] | None:
        if not rows:
            return None
        ws = self.engagements_ws()
        return ws.append_rows(rows, value_input_option="USER_ENTERED")

    @staticmethod
    def _first_row_from_append_response(res: dict[str, Any] | None) -> int | None:
        if not res:
            return None
        r = (res.get("updates") or {}).get("updatedRange") or ""
        m = re.search(r"!([A-Za-z]+)(\d+)", r)
        if not m:
            return None
        return int(m.group(2))

    def append_engagement_dicts(self, records: list[dict[str, Any]]) -> None:
        """Append rows by matching current Engagements headers (row 1)."""
        if not records:
            return
        headers = self.load_engagement_headers()
        idx = self._header_index_map(headers)
        width = len(headers)
        notes: list[str | None] = []
        out: list[list[Any]] = []
        for rec in records:
            note = rec.get(CURRENT_COMPANY_NOTE_KEY)
            notes.append(note if isinstance(note, str) and note.strip() else None)
            row = [""] * width
            for k, v in rec.items():
                if k == CURRENT_COMPANY_NOTE_KEY:
                    continue
                nk = _norm_header(k)
                if nk in idx:
                    row[idx[nk]] = v
            out.append(row)
        res = self.append_engagement_rows(out)
        first_row = self._first_row_from_append_response(res)
        if first_row is None:
            return
        nk_cc = _norm_header("Current Company")
        if nk_cc not in idx:
            return
        col_letter = _column_letter_index(idx[nk_cc])
        to_notes: dict[str, str] = {}
        for i, ntext in enumerate(notes):
            if not ntext:
                continue
            to_notes[f"{col_letter}{first_row + i}"] = ntext
        if to_notes:
            self.engagements_ws().insert_notes(to_notes)

    def update_profile_cell(self, sheet_row: int, header_name: str, value: str) -> None:
        ws = self.profile_ws()
        headers = ws.row_values(1)
        m = self._header_index_map(headers)
        k = _norm_header(header_name)
        if k not in m:
            return
        col_letter = _column_letter_index(m[k])
        ws.update_acell(f"{col_letter}{sheet_row}", value)

    def update_engagement_util_cell(self, sheet_row: int, header_name: str, value: str) -> None:
        ws = self.profiles_engagement_util_ws()
        headers = ws.row_values(1)
        m = self._header_index_map(headers)
        k = _norm_header(header_name)
        if k not in m:
            return
        col_letter = _column_letter_index(m[k])
        ws.update_acell(f"{col_letter}{sheet_row}", value)

    @staticmethod
    def _cell_str(row: list[str], i: int) -> str:
        if len(row) <= i:
            return ""
        x = row[i]
        if isinstance(x, str):
            return x.strip()
        if isinstance(x, (int, float)):
            return str(x).strip()
        return str(x).strip() if x else ""

    def load_engagement_util_index(
        self,
    ) -> tuple[set[str], dict[str, int], dict[str, dict[str, str]]]:
        """
        Scan Profiles_Engagement_Util: normalized profile URL keys, 1-based sheet row per key,
        and per-row fields used as scrape state (urn, last_commented, last_reacted_post_id).
        """
        ws = self.profiles_engagement_util_ws()
        rows = ws.get_all_values(value_render_option="FORMULA")
        if not rows:
            return set(), {}, {}
        headers = rows[0]
        m = self._header_index_map(headers)
        i_url = None
        for nk in ("linkedin profile", "profile url", "linkedin url", "url"):
            if nk in m:
                i_url = m[nk]
                break
        if i_url is None:
            return set(), {}, {}

        def col_idx(*names: str) -> int | None:
            for n in names:
                nn = _norm_header(n)
                if nn in m:
                    return m[nn]
            return None

        i_urn = col_idx("urn")
        i_lc = col_idx("last commented date")
        i_lr = col_idx("last reacted post id")

        keys: set[str] = set()
        row_by_key: dict[str, int] = {}
        fields_by_key: dict[str, dict[str, str]] = {}

        for ri, r in enumerate(rows[1:], start=2):
            if len(r) <= i_url:
                continue
            raw = self._cell_str(r, i_url)
            url = _extract_linkedin_url(raw) or raw
            k = normalize_profile_url_key(url)
            if not k:
                continue
            keys.add(k)
            row_by_key[k] = ri
            fd: dict[str, str] = {}
            if i_urn is not None:
                fd["urn"] = self._cell_str(r, i_urn)
            if i_lc is not None:
                fd["last_commented"] = self._cell_str(r, i_lc)
            if i_lr is not None:
                fd["last_reacted_post_id"] = self._cell_str(r, i_lr)
            fields_by_key[k] = fd
        return keys, row_by_key, fields_by_key

    def find_engagement_util_sheet_row(self, profile_url: str) -> int | None:
        """1-based row number on Profiles_Engagement_Util for this profile URL, if present."""
        _, row_by_key, _ = self.load_engagement_util_index()
        k = normalize_profile_url_key(profile_url)
        if not k:
            return None
        return row_by_key.get(k)

    def count_tracked_profiles(self) -> int:
        headers, data = self.load_profiles_table()
        return len(data)

    def count_scrapeable_profiles(self) -> int:
        """Rows with a LinkedIn /in/ profile URL (same filter as the scraper)."""
        headers, rows = self.load_profiles_table()
        if not headers:
            return 0
        n = 0
        for i, row in enumerate(rows):
            pinfo = self.find_profile_row(headers, i, row)
            url = (pinfo.get("profile_url") or "").lower()
            if url and "linkedin.com/in/" in url:
                n += 1
        return n

    def append_profiles_engagement_util_row(self, name: str, profile_url: str) -> int | None:
        """Append Name and LinkedIn Profile on Profiles_Engagement_Util (row 1 = headers)."""
        ws = self.profiles_engagement_util_ws()
        headers = ws.row_values(1)
        if not headers:
            return None
        m = self._header_index_map(headers)
        row: list[str] = [""] * len(headers)
        for nk in ("name", "full name", "display name"):
            if nk in m:
                row[m[nk]] = name.strip()
                break
        url_set = False
        for nk in ("linkedin profile", "profile url", "linkedin url", "linkedin profile url"):
            if nk in m:
                row[m[nk]] = profile_url.strip()
                url_set = True
                break
        if not url_set:
            return None
        res = ws.append_rows([row], value_input_option="USER_ENTERED")
        return self._first_row_from_append_response(res)

    def continuation_company_row(self, company_name: str, company_url: str | None) -> list[Any]:
        if company_url and company_name:
            cell = _hyperlink_formula(company_url, company_name)
        else:
            cell = company_name or ""
        return ["", "", "", "", "", "", "", cell, ""]

    @staticmethod
    def build_engagement_record(
        engager_link: str,
        engager_name: str,
        engagement_type: str,
        poster_type: str,
        poster_link: str,
        poster_name: str,
        engagement_date: str,
        post_link: str,
        scrape_date: str,
        current_company: str,
        company_link: str | None,
        profile_headline: str,
    ) -> dict[str, Any]:
        engager_cell = _hyperlink_formula(engager_link, engager_name) if engager_link else engager_name
        poster_cell = _hyperlink_formula(poster_link, poster_name) if poster_link else poster_name
        company_cell = (
            _hyperlink_formula(company_link, current_company)
            if (company_link and current_company)
            else current_company
        )
        return {
            "Engager Name": engager_cell,
            "Engagement Type": engagement_type,
            "Poster Type": poster_type,
            "Poster Name": poster_cell,
            # "Engagement Date": engagement_date,
            "Post Date": engagement_date,  # backward-compat if old header still exists
            "Post Link": post_link,
            "Scrape Date": scrape_date,
            "Current Company": company_cell,
            "Profile Headline": profile_headline,
        }
