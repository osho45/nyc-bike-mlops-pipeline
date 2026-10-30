"""Local audit adapter for PyIceberg file:// URIs on Windows (not Spark IO)."""
import os
import re
from pyiceberg.io.pyarrow import PyArrowFileIO


class LocalFileIO(PyArrowFileIO):
    @staticmethod
    def parse_location(location, properties=None):
        scheme, netloc, path = PyArrowFileIO.parse_location(location, properties or {})
        if os.name == 'nt' and scheme == 'file' and re.match(r'^/[A-Za-z]:/', path):
            path = path[1:]
        return scheme, netloc, path
