from __future__ import annotations

import hashlib


def opaque_stat_fingerprint(identity: tuple[int, int, int, int]) -> str:
    """Return the opaque ETag shared by REST tools and transfer relays.

    The public ETag deliberately covers only the stable stat identity used by
    the workspace contract: device, inode, size, and nanosecond mtime.  A
    transfer's SHA-256 remains a separate byte-integrity value.
    """

    raw = ":".join(str(value) for value in identity).encode("ascii")
    return hashlib.sha256(raw).hexdigest()
