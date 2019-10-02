"""Mock Google Cloud Storage."""
import logging

from google.cloud.storage._helpers import _validate_name

import utils.file

log = logging.getLogger(__name__)


class MockHTTPIterator:
    """MockHTTPIterator."""

    def __init__(self, blobs):
        """Init."""
        self._items = blobs

    def __iter__(self):
        """Iteration."""
        for item in self._items:
            yield item


class MockStorageClient:
    """MockStorageClient."""

    def __init__(self):
        """Init."""
        self._bucket = {}

    def get_bucket(self, name: str):
        """Get Bucket."""
        return self._bucket[name]

    def create_bucket(self, name: str):
        """Create bucket."""
        if name in self._bucket:
            raise ValueError
        self._bucket[name] = MockBucket(name)
        return self._bucket[name]

    def list_blobs(self, bucket: str):
        """List blobs."""
        return self.get_bucket(bucket).list_blobs()


class MockBlob:
    """MockBlob."""

    def __init__(self, name: str, bucket):
        """Init."""
        self._name = name
        self._bucket = bucket
        self.content = None

    @property
    def name(self):
        """Property name."""
        return self._name

    def upload_from_filename(self, filename):
        """Upload this blob's contents from the content of a named file."""
        log.debug("mock_blob.upload_from_filename(%s)" % filename)
        self.content = utils.file.read_string(filename)
        self._bucket._add_file(self.name, self)

    def download_to_filename(self, filename):
        """Download the contents of this blob into a named file."""
        utils.file.write_string(filename, self.content)
        log.debug("mock_blob.download_to_filename(%s)" % filename)


class MockBucket:
    """MockBucket."""

    def __init__(self, name: str):
        """Init."""
        name = _validate_name(name)
        self._name = name
        self._blobs = {}

    def list_blobs(self) -> MockHTTPIterator:
        """List blobs."""
        return MockHTTPIterator(list(self._blobs.values()))

    @property
    def name(self) -> str:
        """Property name."""
        return self._name

    def _add_file(self, filename, blob):
        self._blobs[filename] = blob

    def blob(self, blob_name, **kwargs) -> MockBlob:
        """Get blob."""
        log.debug("mock_bucket.bucket(%s)" % blob_name)
        if blob_name not in self._blobs:
            self._blobs[blob_name] = MockBlob(blob_name, self)
        return self._blobs[blob_name]
