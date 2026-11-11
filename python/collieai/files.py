"""Files: upload a file for analysis, read its metadata, release it
(``POST/GET/DELETE /v1/files``). Preview — the deployment must have file
analysis enabled; otherwise every call raises
:class:`~collieai.errors.FileAnalysisUnavailable`.

Attach an uploaded file to an input check with
``collie.moderate.input(prompt, attachments=[file.id])``.
"""
from __future__ import annotations

import mimetypes
import os
from typing import TYPE_CHECKING, Union

from .errors import CollieAPIError
from .models import FileObject

if TYPE_CHECKING:  # pragma: no cover
    from ._client import AsyncCollie

_SDK_ORIGIN = "files"


class FilesClient:
    def __init__(self, client: "AsyncCollie") -> None:
        self._c = client

    async def upload(self, path: Union[str, "os.PathLike[str]"], *, retain: bool = False) -> FileObject:
        """Upload the file at ``path``. ``retain=True`` keeps the bytes for the
        upload TTL after its last job releases it; otherwise they go as soon as
        no job needs them. The request carries a ``Content-Length`` (the API
        refuses a body without one)."""
        name = os.path.basename(os.fspath(path))
        media_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
        with open(path, "rb") as fh:
            resp = await self._c._request_raw(
                "POST", "/v1/files", sdk_origin=_SDK_ORIGIN,
                files={"file": (name, fh, media_type)},
                data={"retain": "true" if retain else "false"},
            )
        return _file_object(self._c._json_or_error(resp), resp.status_code)

    async def get(self, file_id: str) -> FileObject:
        """The file's metadata (``state``: uploaded, leased, deleted, …)."""
        data, resp = await self._c._request_json("GET", f"/v1/files/{file_id}", sdk_origin=_SDK_ORIGIN)
        return _file_object(data, resp.status_code)

    async def delete(self, file_id: str) -> None:
        """Release the file. Its bytes go once no job needs them; a job that
        already holds it still finishes."""
        await self._c._request_raw("DELETE", f"/v1/files/{file_id}", sdk_origin=_SDK_ORIGIN)



def _file_object(data, status_code: int) -> FileObject:
    """A typed error, never a raw ValidationError, for a malformed body."""
    try:
        return FileObject.model_validate(data)
    except Exception as exc:  # noqa: BLE001
        raise CollieAPIError("API returned a malformed file object", status_code=status_code,
                             code="invalid_response") from exc
