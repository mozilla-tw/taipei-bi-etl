from io import StringIO
import pytest
import logging

log = logging.getLogger(__name__)


class MockIO(StringIO):
    files = {}

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        return self

    def read(self, *args, **kwargs):
        log.warning("mock_io.read")
        return super(MockIO, self).read(*args, **kwargs)

    def write(self, *args, **kwargs):
        log.warning("mock_io.write")
        super(MockIO, self).write(*args, **kwargs)

    def open(self, file, mode='r', **options):
        log.warning("MockIO.open(%s, %s)" % (file, mode))
        if file not in self.files:
            buf = StringIO()
            buf.close = lambda: None
            self.files[file] = buf

        buf = self.files[file]
        buf.seek(0)

        return buf
