"""Entry point for LinkedIn Engagement Tracker."""

import sys
from PyQt6.QtWidgets import QApplication
from PyQt6.QtGui import QIcon
from app.ui import _window_icon_path, MainWindow

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    ip = _window_icon_path()
    if ip is not None:
        app.setWindowIcon(QIcon(str(ip)))
    w = MainWindow()
    w.show()
    sys.exit(app.exec())
