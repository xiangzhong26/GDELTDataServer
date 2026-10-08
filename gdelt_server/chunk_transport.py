"""Content-defined gzip members: ordinary gzip downloads remain backwards compatible.

Boundaries follow stable JSON rows rather than byte offsets. Small edits therefore
do not invalidate all following chunks. Hashes describe compressed bytes exactly.
"""
import gzip
import hashlib
import re
import zlib

MIN_CHUNK = 256 * 1024
MAX_CHUNK = 2 * 1024 * 1024


def compress_chunks(raw):
    parts, index = [], []
    start = offset = 0
    for match in re.finditer(br'\],\[|\}\},"', raw):
        end = match.end() - 1
        length = end - start
        if length < MIN_CHUNK:
            continue
        if length < MAX_CHUNK and zlib.crc32(raw[end:end + 128]) & 1023:
            continue
        part = gzip.compress(raw[start:end], compresslevel=6, mtime=0)
        parts.append(part)
        index.append({'sha256': hashlib.sha256(part).hexdigest(), 'offset': offset, 'bytes': len(part)})
        offset += len(part)
        start = end
    part = gzip.compress(raw[start:], compresslevel=6, mtime=0)
    parts.append(part)
    index.append({'sha256': hashlib.sha256(part).hexdigest(), 'offset': offset, 'bytes': len(part)})
    return b''.join(parts), index
