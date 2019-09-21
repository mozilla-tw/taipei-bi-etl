"""Mock File IO."""
from io import StringIO
import logging

log = logging.getLogger(__name__)


class MockIO(StringIO):
    """Mock Object Class for file io."""

    files = {}

    def __enter__(self, *args, **kwargs):
        """Enter."""
        return self

    def __exit__(self, *args, **kwargs):
        """Exit."""
        return self

    def read(self, *args, **kwargs):
        """Read."""
        log.warning("mock_io.read")
        return super(MockIO, self).read(*args, **kwargs)

    def write(self, *args, **kwargs):
        """Write."""
        log.warning("mock_io.write")
        super(MockIO, self).write(*args, **kwargs)

    def open(self, file, mode="r", **options):
        """Open."""
        log.warning("MockIO.open(%s, %s)" % (file, mode))
        if file not in self.files:
            buf = StringIO()
            buf.close = lambda: None
            self.files[file] = buf

        buf = self.files[file]
        buf.seek(0)

        return buf
