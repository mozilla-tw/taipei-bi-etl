"""Mock File IO."""
from io import StringIO
import logging

log = logging.getLogger(__name__)


class MockIO:
    """Mock Object Class for file io."""

    files = {}

    def open(self, file, mode="r", **options):
        """Open."""
        log.debug("MockIO.open(%s, %s)" % (file, mode))
        if file not in self.files:
            buf = StringIO()
            buf.close = lambda: None
            self.files[file] = buf

        buf = self.files[file]
        buf.seek(0)

        return buf


class MockReadWrite:
    """Mock Object Class for read/write help functions."""

    def __init__(self):
        """Init."""
        self.files = {}

    def read_string(self, path: str) -> str:
        """Read."""
        return self.files[path]

    def write_string(self, path: str, s: str):
        """Write."""
        self.files[path] = s
