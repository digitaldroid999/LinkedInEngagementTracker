"""PyQt6 main window: scheduling, run, status bar."""

from __future__ import annotations

import logging
import sys
import traceback
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any

from PyQt6.QtCore import QSettings, QSize, Qt, QThread, QTime, QTimer, pyqtSignal
from PyQt6.QtGui import QCloseEvent, QFont, QIcon
from PyQt6.QtWidgets import (
    QApplication,
    QButtonGroup,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QSizePolicy,
    QTimeEdit,
    QVBoxLayout,
    QWidget,
)

from app.config import GOOGLE_SHEET_ID, load_credentials
from app.linkedin_api import LinkedInAPIClient
from app.scrape_log import LOGGER_NAME, begin_scrape_session
from app.scraper import EngagementScraper, ScrapeStats
from app.sheets import SheetsManager

_log = logging.getLogger(f"{LOGGER_NAME}.ui")


SHEET_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}"

# Fixed main window size (not user-resizable).
MAIN_WINDOW_SIZE = QSize(650, 400)


def _window_icon_path() -> Path | None:
    """icon.ico next to this module, or bundled under PyInstaller onefile."""
    if getattr(sys, "frozen", False):
        bundled = Path(sys._MEIPASS) / "icon.ico"
        if bundled.is_file():
            return bundled
    p = Path(__file__).resolve().parent.parent / "icon.ico"
    return p if p.is_file() else None


def _apply_main_window_flags(w: QWidget) -> None:
    """Title bar without maximize (Windows ignores clearing MaximizeButtonHint on defaults)."""
    flags = (
        Qt.WindowType.Window
        | Qt.WindowType.WindowTitleHint
        | Qt.WindowType.WindowCloseButtonHint
        | Qt.WindowType.WindowMinimizeButtonHint
        | Qt.WindowType.WindowSystemMenuHint
    )
    if sys.platform == "win32":
        flags |= Qt.WindowType.MSWindowsFixedSizeDialogHint
    w.setWindowFlags(flags)
    w.setWindowFlag(Qt.WindowType.WindowMaximizeButtonHint, False)


def next_scheduled_datetime(weekday: int, hour: int, minute: int) -> datetime:
    """Return next local datetime on weekday (Mon=0..Sun=6) at hour:minute."""
    now = datetime.now()
    for delta in range(0, 14):
        d = (now.date() + timedelta(days=delta))
        if d.weekday() != weekday:
            continue
        t = datetime.combine(d, dt_time(hour, minute))
        if t > now:
            return t
    return now + timedelta(days=7)


def format_next_line(weekday: int, hour: int, minute: int) -> str:
    n = next_scheduled_datetime(weekday, hour, minute)
    tm = n.strftime("%I:%M %p").lstrip("0")
    return f"Next auto-scrape is at {tm} on {n:%A, %B %d, %Y}"


class ScrapeWorker(QThread):
    progress = pyqtSignal(str, int, int, int, int)
    finished_ok = pyqtSignal(object)
    failed = pyqtSignal(str)

    def __init__(self, parent: QWidget | None = None, silent: bool = False) -> None:
        super().__init__(parent)
        self.silent = silent

    def run(self) -> None:
        log_path = begin_scrape_session()
        _log.info("Scrape session started; log file: %s", log_path)
        try:
            g, key = load_credentials()
        except Exception as e:
            _log.error("Credential load failed: %s", e)
            self.failed.emit(str(e))
            return
        try:
            api = LinkedInAPIClient(key)
            sheets = SheetsManager(g)

            def cb(name: str, nc: int, nr: int, idx: int, total: int) -> None:
                self.progress.emit(name, nc, nr, idx, total)

            def should_stop() -> bool:
                return self.isInterruptionRequested()

            scraper = EngagementScraper(api, sheets, progress_cb=cb, stop_check=should_stop)
            stats = scraper.run()
            _log.info(
                "Scrape session completed successfully (log: %s). comments=%s reactions=%s",
                log_path,
                stats.new_comments,
                stats.new_reactions,
            )
            self.finished_ok.emit(stats)
        except Exception as e:
            _log.error("Scrape session crashed: %s\n%s", e, traceback.format_exc())
            self.failed.emit(str(e))


class MainWindow(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("LinkedIn Engagement Tracker")
        _apply_main_window_flags(self)
        self._settings = QSettings("LinkedInEngagementTracker", "App")
        self._worker: ScrapeWorker | None = None
        self._running = False
        self._profile_count = 0
        self._silent_completion = False

        self._build_ui()
        self.setFixedSize(MAIN_WINDOW_SIZE)
        ip = _window_icon_path()
        if ip is not None:
            self.setWindowIcon(QIcon(str(ip)))
        self._load_settings_ui()
        self._refresh_profile_count()
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._on_timer)
        self._timer.start(1000)
        self._refresh_status_idle()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setSpacing(12)
        root.setContentsMargins(16, 16, 16, 16)

        row1 = QHBoxLayout()
        self._lbl_count = QLabel("Profiles in tracking: —")
        f = QFont()
        f.setPointSize(11)
        f.setBold(True)
        self._lbl_count.setFont(f)
        row1.addWidget(self._lbl_count)
        row1.addStretch()
        #row1.addWidget(QLabel("Google Sheets link"))
        self._btn_copy = QPushButton("Copy")
        self._btn_copy.setFixedWidth(72)
        self._btn_copy.clicked.connect(self._copy_sheet_url)
        self._btn_copy.hide()
        row1.addWidget(self._btn_copy)
        root.addLayout(row1)

        settings_box = QGroupBox("Settings")
        s_layout = QVBoxLayout(settings_box)

        sched_box = QGroupBox("Weekly auto-scrape")
        sched_box.setObjectName("weeklyAutoScrapeBox")
        sched_layout = QVBoxLayout(sched_box)
        sched_layout.setContentsMargins(16, 20, 16, 16)
        sched_layout.setSpacing(18)

        lbl_days = QLabel("Day of week")
        lbl_days.setObjectName("weeklySectionLabel")
        sched_layout.addWidget(lbl_days)

        days_wrap = QWidget()
        days_wrap.setObjectName("weeklyDaysWrap")
        days_row = QHBoxLayout(days_wrap)
        days_row.setContentsMargins(0, 0, 0, 0)
        days_row.setSpacing(20)
        self._day_group = QButtonGroup(self)
        day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        self._day_radios: list[QRadioButton] = []
        for i, name in enumerate(day_names):
            rb = QRadioButton(name)
            rb.setObjectName("weeklyDayRadio")
            self._day_radios.append(rb)
            self._day_group.addButton(rb, i)
            days_row.addWidget(rb)
        days_row.addStretch()
        sched_layout.addWidget(days_wrap)

        lbl_time = QLabel("Schedule time")
        lbl_time.setObjectName("weeklySectionLabel")
        sched_layout.addWidget(lbl_time)

        time_row = QHBoxLayout()
        time_row.setSpacing(12)
        self._time_edit = QTimeEdit()
        self._time_edit.setDisplayFormat("h:mm AP")
        self._time_edit.setCalendarPopup(False)
        self._time_edit.setMinimumHeight(30)
        time_row.addWidget(self._time_edit)
        time_row.addStretch()
        sched_layout.addLayout(time_row)

        for rb in self._day_radios:
            rb.toggled.connect(self._on_schedule_changed)
        self._time_edit.timeChanged.connect(lambda _t: self._on_schedule_changed())

        s_layout.addWidget(sched_box)
        root.addWidget(settings_box)

        run_row = QHBoxLayout()
        run_row.setSpacing(12)
        run_row.addStretch()
        self._btn_run = QPushButton("Run")
        self._btn_run.setMinimumWidth(140)
        self._btn_run.setMinimumHeight(36)
        self._btn_run.clicked.connect(self._on_run_clicked)
        run_row.addWidget(self._btn_run)
        self._btn_stop = QPushButton("Stop")
        self._btn_stop.setObjectName("stopBtn")
        self._btn_stop.setMinimumWidth(140)
        self._btn_stop.setMinimumHeight(36)
        self._btn_stop.setEnabled(False)
        self._btn_stop.clicked.connect(self._on_stop_clicked)
        run_row.addWidget(self._btn_stop)
        run_row.addStretch()
        root.addLayout(run_row)

        self._status_left = QLabel("")
        self._status_right = QLabel("")
        self._status_left.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self._status_right.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        bar = QHBoxLayout()
        bar.addWidget(self._status_left, 1)
        bar.addWidget(self._status_right, 1)
        root.addLayout(bar)

        self.setStyleSheet(
            """
            QWidget { background-color: #f6f7fb; color: #1a1a2e; }
            QGroupBox {
                font-weight: 600;
                border: 1px solid #d8dbe7;
                border-radius: 8px;
                margin-top: 10px;
                padding: 12px 10px 10px 10px;
                background-color: #ffffff;
                font-size: 13px;
            }
            QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 6px; }
            QGroupBox#weeklyAutoScrapeBox {
                border: 1px solid #c7d2fe;
                background-color: #f8faff;
                margin-top: 6px;
            }
            QLabel#weeklySectionLabel {
                color: #64748b;
                font-size: 12px;
                font-weight: 600;
                letter-spacing: 0.02em;
            }
            QWidget#weeklyDaysWrap {
                background-color: transparent;
            }
            QRadioButton#weeklyDayRadio {
                spacing: 8px;
                padding: 8px 6px;
                min-width: 48px;
            }
            QRadioButton#weeklyDayRadio::indicator {
                width: 16px;
                height: 16px;
            }
            QPushButton {
                background-color: #2563eb;
                color: white;
                border: none;
                border-radius: 6px;
                padding: 6px 12px;
                font-weight: 600;
            }
            QPushButton:hover { background-color: #1d4ed8; }
            QPushButton:disabled { background-color: #94a3b8; }
            QPushButton#copyBtn { background-color: #334155; }
            QPushButton#copyBtn:hover { background-color: #1e293b; }
            QPushButton#stopBtn {
                background-color: #b91c1c;
            }
            QPushButton#stopBtn:hover { background-color: #991b1b; }
            QPushButton#stopBtn:disabled { background-color: #94a3b8; }
            QRadioButton { spacing: 6px; }
            QGroupBox#weeklyAutoScrapeBox QTimeEdit {
                min-width: 120px;
                padding: 6px 8px;
            }
            QTimeEdit { padding: 4px 5px; border-radius: 4px; border: 1px solid #cbd5e1; background: #fff; }
            """
        )
        self._btn_copy.setObjectName("copyBtn")

    def _load_settings_ui(self) -> None:
        day = int(self._settings.value("schedule/weekday", 0) or 0)
        day = max(0, min(6, day))
        self._day_radios[day].setChecked(True)
        h = int(self._settings.value("schedule/hour", 9) or 9)
        m = int(self._settings.value("schedule/minute", 0) or 0)
        self._time_edit.setTime(QTime(h, m))

    def _save_settings(self) -> None:
        wd = self._day_group.checkedId()
        if wd < 0:
            wd = 0
        t = self._time_edit.time()
        self._settings.setValue("schedule/weekday", wd)
        self._settings.setValue("schedule/hour", t.hour())
        self._settings.setValue("schedule/minute", t.minute())

    def _on_schedule_changed(self) -> None:
        self._save_settings()
        if not self._running:
            self._refresh_status_idle()

    def _schedule_params(self) -> tuple[int, int, int]:
        wd = self._day_group.checkedId()
        if wd < 0:
            wd = 0
        t = self._time_edit.time()
        return wd, t.hour(), t.minute()

    def _refresh_profile_count(self) -> None:
        try:
            g, _ = load_credentials()
            sm = SheetsManager(g)
            self._profile_count = sm.count_scrapeable_profiles()
            self._lbl_count.setText(f"Profiles in tracking: {self._profile_count}")
        except Exception:
            self._profile_count = 0
            self._lbl_count.setText("Profiles in tracking: 0")

    def _copy_sheet_url(self) -> None:
        QApplication.clipboard().setText(SHEET_URL)
        QMessageBox.information(self, "Copied", "Google Sheets link copied to clipboard.")

    def _refresh_status_idle(self) -> None:
        wd, h, m = self._schedule_params()
        self._status_left.setText("")
        self._status_right.setText(format_next_line(wd, h, m))

    def _refresh_status_running(
        self, name: str, nc: int, nr: int, idx: int = 0, total: int = 0
    ) -> None:
        self._status_left.setText(f"Scraping: {name}")
        self._status_right.setText(f"Found: {nc} new comments, and {nr} new reactions")
        if total > 0:
            if idx > 0:
                self._lbl_count.setText(
                    f"Profiles in tracking: {self._profile_count} - {idx}/{total} in progress"
                )
            else:
                self._lbl_count.setText(
                    f"Profiles in tracking: {self._profile_count} - starting ({total} to scrape)"
                )

    def _on_timer(self) -> None:
        if self._running:
            return
        self._refresh_status_idle()
        wd, h, m = self._schedule_params()
        now = datetime.now()
        if now.weekday() != wd:
            return
        if now.hour != h or now.minute != m:
            return
        self._refresh_profile_count()
        if self._profile_count == 0:
            return
        last = self._settings.value("auto/last_run_minute_key", "")
        key = now.strftime("%Y-%m-%d-%H-%M")
        if last == key:
            return
        self._settings.setValue("auto/last_run_minute_key", key)
        self._start_scrape(auto=True)

    def _on_run_clicked(self) -> None:
        self._start_scrape(auto=False)

    def _on_stop_clicked(self) -> None:
        if self._worker and self._worker.isRunning():
            self._worker.requestInterruption()

    def _start_scrape(self, auto: bool = False) -> None:
        if self._running:
            return
        self._refresh_profile_count()
        if self._profile_count == 0:
            QMessageBox.information(
                self,
                "Nothing to scrape",
                "No LinkedIn profiles to scrape. Add at least one row in the Profiles sheet "
                "with a profile URL containing linkedin.com/in/.",
            )
            return
        self._running = True
        self._silent_completion = auto
        self._btn_run.setEnabled(False)
        self._btn_run.setText("Running")
        self._btn_stop.setEnabled(True)
        self._refresh_status_running("Starting…", 0, 0, 0, self._profile_count)
        self._worker = ScrapeWorker(self, silent=auto)
        self._worker.progress.connect(self._on_worker_progress)
        self._worker.finished_ok.connect(self._on_worker_done)
        self._worker.failed.connect(self._on_worker_fail)
        self._worker.finished.connect(self._on_worker_thread_finished)
        self._worker.start()

    def _on_worker_progress(self, name: str, nc: int, nr: int, idx: int, total: int) -> None:
        self._refresh_status_running(name, nc, nr, idx, total)

    def _on_worker_done(self, stats: Any) -> None:
        self._refresh_profile_count()
        s = stats if isinstance(stats, ScrapeStats) else ScrapeStats()
        if self._silent_completion:
            title = "Auto-scrape stopped" if s.stopped else "Auto-scrape complete"
        else:
            title = "Scrape stopped" if s.stopped else "Scrape complete"
        if s.stopped:
            msg = (
                "Scrape was stopped after the profile that was in progress finished.\n\n"
                f"New comments: {s.new_comments}, new reactions: {s.new_reactions}."
            )
        else:
            msg = f"Finished. New comments: {s.new_comments}, new reactions: {s.new_reactions}."
        if s.errors:
            msg += "\n\nWarnings:\n" + "\n".join(s.errors[:12])
            if len(s.errors) > 12:
                msg += f"\n… and {len(s.errors) - 12} more."
        QMessageBox.information(self, title, msg)

    def _on_worker_fail(self, err: str) -> None:
        if self._silent_completion:
            QMessageBox.critical(self, "Auto-scrape failed", err)
        else:
            QMessageBox.critical(self, "Error", err)

    def _on_worker_thread_finished(self) -> None:
        self._running = False
        self._btn_run.setEnabled(True)
        self._btn_run.setText("Run")
        self._btn_stop.setEnabled(False)
        self._worker = None
        self._refresh_profile_count()
        self._refresh_status_idle()

    def closeEvent(self, event: QCloseEvent | None) -> None:
        if self._running:
            QMessageBox.warning(
                self,
                "Scraping in progress",
                "A scraper is still running. Stop waits for the current profile to finish, or wait until the run ends before closing.",
            )
            if event:
                event.ignore()
            return
        if event:
            event.accept()
    
