<<<<<<< HEAD

from __future__ import annotations
import os
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import urllib.parse
import httpx

log = logging.getLogger("rddms-admin.osdu")

# ───────────────────────────────────────────────────────────────────────────────
# Environment & defaults
# ───────────────────────────────────────────────────────────────────────────────

# Base DNS name of your ADME/OSDU instance (no scheme).
OSDU_BASE_URL: str = os.getenv("OSDU_BASE_URL", "equinordev.energy.azure.com")

# Required header for all ADME/OSDU calls.
DATA_PARTITION_ID: str = os.getenv("DATA_PARTITION_ID", "").strip()

def _partition_suffix() -> str:
    # e.g., "dp1.dataservices.energy"
    return f"{DATA_PARTITION_ID}.dataservices.energy" if DATA_PARTITION_ID else "partition.dataservices.energy"

# Sensible defaults for the "Create Dataspace" form (can be overridden in env)
DEFAULT_LEGAL_TAG: str = os.getenv(
    "DEFAULT_LEGAL_TAG",
    f"{DATA_PARTITION_ID}-RDDMS-Legal-Tag" if DATA_PARTITION_ID else "dp1-RDDMS-Legal-Tag"
)
_default_owners = os.getenv("DEFAULT_OWNERS", f"data.default.owners@{_partition_suffix()}")
DEFAULT_OWNERS: List[str] = [x.strip() for x in _default_owners.split(",") if x.strip()]
_default_viewers = os.getenv("DEFAULT_VIEWERS", f"data.default.viewers@{_partition_suffix()}")
DEFAULT_VIEWERS: List[str] = [x.strip() for x in _default_viewers.split(",") if x.strip()]
_default_countries = os.getenv("DEFAULT_COUNTRIES", "US")
DEFAULT_COUNTRIES: List[str] = [x.strip() for x in _default_countries.split(",") if x.strip()]

# ───────────────────────────────────────────────────────────────────────────────
# HTTP utils
# ───────────────────────────────────────────────────────────────────────────────

def headers(access_token: str) -> Dict[str, str]:
    if not DATA_PARTITION_ID:
        log.warning("DATA_PARTITION_ID env var is not set; calls may fail")
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "data-partition-id": DATA_PARTITION_ID,
    }

def _normalize_base_url(base: str) -> str:
    # Accept both "host" and "https://host"
    return base if base.startswith("http") else f"https://{base}"

# ───────────────────────────────────────────────────────────────────────────────
# Dataspaces
# ───────────────────────────────────────────────────────────────────────────────

async def list_dataspaces(access_token: str) -> List[Dict[str, Any]]:
    """GET /api/reservoir-ddms/v2/dataspaces"""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or []

async def create_dataspace(
    access_token: str,
    path: str,
    *,
    legal_tag: str,
    owners: List[str],
    viewers: List[str],
    countries: List[str],
    extra_custom: Optional[Dict[str, Any]] = None,
) -> Any:
    """POST /api/reservoir-ddms/v2/dataspaces"""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces"
    custom: Dict[str, Any] = {
        "legaltags": [legal_tag],
        "otherRelevantDataCountries": countries,
        "viewers": viewers,
        "owners": owners,
    }
    if extra_custom:
        # Do not let extra keys override reserved compliance ACL fields
        for k in ("legaltags", "otherRelevantDataCountries", "viewers", "owners"):
            extra_custom.pop(k, None)
        custom.update(extra_custom)

    payload = [
        {
            "DataspaceId": path,
            "Path": path,
            "CustomData": custom,
        }
    ]
    hdr = headers(access_token)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=hdr, json=payload)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
            corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
            log.error(
                "Dataspace create failed (%s) corr=%s\nURL=%s\nHeaders=%s\nPayload=%s\nResponseHeaders=%s\nBody=%s",
                r.status_code, corr, url, hdr, json.dumps(payload, indent=2),
                dict(r.headers), r.text
            )
            raise
        return r.json()

async def delete_dataspace(access_token: str, path: str) -> None:
    """
    DELETE /api/reservoir-ddms/v2/dataspaces/{dataspaceId}
    """
    enc = urllib.parse.quote(path, safe="")
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{enc}"
    hdr = headers(access_token)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.delete(url, headers=hdr)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
            corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
            log.error("Dataspace delete failed (%s) corr=%s path=%s body=%s", r.status_code, corr, path, r.text)
            raise

async def lock_dataspace(access_token: str, path: str) -> None:
    """
    POST /api/reservoir-ddms/v2/dataspaces/{dataspaceId}/lock
    """
    enc = urllib.parse.quote(path, safe="")
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{enc}/lock"
    hdr = headers(access_token)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=hdr)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
            corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
            log.error("Dataspace lock failed (%s) corr=%s path=%s body=%s", r.status_code, corr, path, r.text)
            raise

async def unlock_dataspace(access_token: str, path: str) -> None:
    """
    DELETE /api/reservoir-ddms/v2/dataspaces/{dataspaceId}/lock
    """
    enc = urllib.parse.quote(path, safe="")
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{enc}/lock"
    hdr = headers(access_token)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.delete(url, headers=hdr)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
            corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
            log.error("Dataspace unlock failed (%s) corr=%s path=%s body=%s", r.status_code, corr, path, r.text)
            raise

def _dataspace_uri(path: str) -> str:
    # Canonical form seen in responses: eml:///dataspace('demo/Volve')
    return f"eml:///dataspace('{path}')"

# ───────────────────────────────────────────────────────────────────────────────
# Types & resources
# ───────────────────────────────────────────────────────────────────────────────

async def list_types(access_token: str, ds_enc: str) -> List[Dict[str, Any]]:
    """GET /dataspaces/{dataspaceId}/resources -> list of {'name','count'}"""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or []

async def list_resources(access_token: str, ds_enc: str, typ: str) -> List[Dict[str, Any]]:
    """GET /dataspaces/{dataspaceId}/resources/{dataObjectType}"""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources/{typ}"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or []

async def get_resource(
    access_token: str,
    ds_enc: str,
    typ: str,
    uuid: str,
    *,
    include_refs: bool = False,  # reserved for future expansion
) -> Dict[str, Any]:
    """GET /dataspaces/{dataspaceId}/resources/{dataObjectType}/{guid}"""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources/{typ}/{uuid}"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or {}

async def list_arrays(access_token: str, ds_enc: str, typ: str, uuid: str) -> List[Dict[str, Any]]:
    """GET arrays metadata list for an object."""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources/{typ}/{uuid}/arrays"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or []

async def read_array(
    access_token: str,
    ds_enc: str,
    typ: str,
    uuid: str,
    *,
    path_in_resource: str,
) -> Dict[str, Any]:
    """GET content of an array."""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources/{typ}/{uuid}/arrays/{path_in_resource}"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or {}

# ───────────────────────────────────────────────────────────────────────────────
# Helpers for UI features
# ───────────────────────────────────────────────────────────────────────────────

def extract_refs(obj: Dict[str, Any]) -> List[Dict[str, str]]:
    """Very lightweight scan for DataObjectReference-like dicts."""
    edges: List[Dict[str, str]] = []
    def _walk(x: Any):
        if isinstance(x, dict):
            ct = x.get("ContentType")
            uid = x.get("UUID") or x.get("Uuid")
            if ct and uid:
                edges.append({"contentType": ct, "uuid": str(uid)})
            for v in x.values():
                _walk(v)
        elif isinstance(x, list):
            for v in x:
                _walk(v)
    _walk(obj)
    return edges

def extract_grid2d_geometry(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract minimal visualization metadata from a Grid2dRepresentation."""
    if not (obj.get("$type", "") or "").endswith("Grid2dRepresentation"):
        return None
    try:
        patch = obj["Grid2dPatch"]
        fast = int(patch["FastestAxisCount"])
        slow = int(patch["SlowestAxisCount"])
        geom = patch["Geometry"]
        pts = geom["Points"]
        origin = pts["Origin"]
        offsets = pts["Offset"]
        u = offsets[0]
        v = offsets[1]
        return {
            "fast": fast,
            "slow": slow,
            "origin": {
                "x": origin.get("Coordinate1", 0.0),
                "y": origin.get("Coordinate2", 0.0),
                "z": origin.get("Coordinate3", 0.0),
            },
            "u": {
                "dx": (u.get("Offset") or {}).get("Coordinate1", 0.0),
                "dy": (u.get("Offset") or {}).get("Coordinate2", 0.0),
                "spacing": ((u.get("Spacing") or {}).get("Value", 1.0)),
            },
            "v": {
                "dx": (v.get("Offset") or {}).get("Coordinate1", 0.0),
                "dy": (v.get("Offset") or {}).get("Coordinate2", 0.0),
                "spacing": ((v.get("Spacing") or {}).get("Value", 1.0)),
            },
        }
    except Exception:
        return None

# ───────────────────────────────────────────────────────────────────────────────
# URI helpers for manifest build
# ───────────────────────────────────────────────────────────────────────────────

async def list_all_resource_uris(access_token: str, ds_enc: str) -> List[str]:
    """
    GET /dataspaces/{dataspaceId}/resources/all -> collect 'uri' strings for manifest build.
    """
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources/all"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        rows = r.json() or []
    uris: List[str] = []
    for x in rows:
        uri = x.get("uri") if isinstance(x, dict) else None
        if uri:
            uris.append(uri)
    return uris

def _type_from_content_type(ct: str) -> Optional[str]:
    """
    Convert e.g. 'application/x-resqml+xml;version=2.0;type=obj_LocalDepth3dCrs'
    into 'resqml20.obj_LocalDepth3dCrs'.
    """
    if not ct:
        return None
    parts = ct.split("type=")
    if len(parts) < 2:
        return None
    t = parts[1].strip()
    if t.startswith("obj_"):
        return f"resqml20.{t}"
    if "resqml20.obj_" in t:
        idx = t.find("resqml20.obj_")
        return t[idx:].strip()
    return None

def uri_from_ref(ds_path: str, content_type: str, uuid: str) -> Optional[str]:
    """
    Build an EML URI for a reference in the same dataspace:
      eml:///dataspace('<ds>')/<resqml20.obj_*>(<uuid>)
    """
    typ = _type_from_content_type(content_type)
    if not typ or not uuid:
        return None
    return f"eml:///dataspace('{ds_path}')/{typ}({uuid})"

async def gather_selected_uris_with_refs(
    access_token: str,
    ds_path: str,
    selections: List[Tuple[str, str]],  # (type, uuid)
    include_refs: bool = True,
) -> List[str]:
    """
    For selected objects, gather their URIs and (optional) immediate references' URIs (e.g., CRS).
    """
    uris: List[str] = []
    ds_enc = urllib.parse.quote(ds_path, safe="")
    for typ, uuid in selections:
        primary_uri = f"eml:///dataspace('{ds_path}')/{typ}({uuid})"
        uris.append(primary_uri)
        if include_refs:
            obj = await get_resource(access_token, ds_enc, typ, uuid, include_refs=True)
            for ref in extract_refs(obj):
                ref_uri = uri_from_ref(ds_path, ref.get("contentType", ""), ref.get("uuid", ""))
                if ref_uri:
                    uris.append(ref_uri)
    # unique preserve order
    seen = set()
    out: List[str] = []
    for u in uris:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

# ───────────────────────────────────────────────────────────────────────────────
# Manifest build (supports explicit URIs)
# ───────────────────────────────────────────────────────────────────────────────

async def build_manifest(
    access_token: str,
    path: str,
    *,
    legal_tag: str | None = None,
    owners: list[str] | None = None,
    viewers: list[str] | None = None,
    countries: list[str] | None = None,
    create_missing_refs: bool = True,
    use_all_resources: bool = False,                # default False: we often pass explicit URIs
    explicit_uris: Optional[List[str]] = None,      # when provided, overrides 'uris'
) -> dict:
    """
    POST /api/reservoir-ddms/v2/manifests/build
    Body includes: uris[], acl{}, legal{}, createMissingReferences.
    """
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/manifests/build"
    hdr = headers(access_token)

    # Defaults
    legal_tag = legal_tag or DEFAULT_LEGAL_TAG
    owners = owners or DEFAULT_OWNERS
    viewers = viewers or DEFAULT_VIEWERS
    countries = countries or DEFAULT_COUNTRIES

    # Resolve URIs
    if explicit_uris and len(explicit_uris) > 0:
        uris = explicit_uris
    elif use_all_resources:
        ds_enc = urllib.parse.quote(path, safe="")
        uris = await list_all_resource_uris(access_token, ds_enc)
        if not uris:
            log.warning("No resources for '%s'; falling back to dataspace URI", path)
            uris = [_dataspace_uri(path)]
    else:
        uris = [_dataspace_uri(path)]

    body = {
        "uris": uris,
        "acl": {
            "owners": owners,
            "viewers": viewers,
        },
        "legal": {
            "legaltags": [legal_tag],
            "otherRelevantDataCountries": countries,
        },
        "createMissingReferences": bool(create_missing_refs),
    }

    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(url, headers=hdr, json=body)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
            corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
            log.error("Build manifest failed (%s) corr=%s path=%s body=%s", r.status_code, corr, path, r.text)
            raise
    return r.json() or {}

# ───────────────────────────────────────────────────────────────────────────────
# Workflow submit (Osdu_ingest)
# ───────────────────────────────────────────────────────────────────────────────

async def submit_workflow_run(
    *,
    access_token: str,
    manifest: Dict[str, Any],
    partition: Optional[str] = None,
    app_key: Optional[str] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    POST /api/workflow/v1/workflow/Osdu_ingest/workflowRun
    """
    base = _normalize_base_url(OSDU_BASE_URL).rstrip('/')
    url = base + "/api/workflow/v1/workflow/Osdu_ingest/workflowRun"

    part = (partition or DATA_PARTITION_ID or "data")
    headers_wf = {
        "Authorization": f"Bearer {access_token}",
        "data-partition-id": part,
        "Content-Type": "application/json",
    }
    if app_key:
        headers_wf["AppKey"] = app_key

    payload = {
        "executionContext": {
            "Payload": {
                "data-partition-id": part,
                **({"AppKey": app_key} if app_key else {}),
            },
            "manifest": manifest,
        },
        **({"runId": run_id} if run_id else {}),
    }

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=60.0)) as client:
        r = await client.post(url, headers=headers_wf, json=payload)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
            corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
            log.error("Workflow run failed (%s) corr=%s body=%s", r.status_code, corr, r.text[:2000])
            raise
        try:
            return r.json() or {}
        except Exception:
            return {"status_code": r.status_code, "text": r.text}
=======
from __future__ import annotations

import os
import json
import logging
from typing import Any, Dict, List, Optional
import urllib.parse  
import httpx

log = logging.getLogger("rddms-admin.osdu")

# ----------------------------------------------------------------------
# Environment & defaults
# ----------------------------------------------------------------------

# Base DNS name of your ADME/OSDU instance (no scheme).
OSDU_BASE_URL: str = os.getenv("OSDU_BASE_URL", "equinordev.energy.azure.com")

# Required header for all ADME/OSDU calls.
DATA_PARTITION_ID: str = os.getenv("DATA_PARTITION_ID", "").strip()

def _partition_suffix() -> str:
    # e.g., "dp1.dataservices.energy"
    return f"{DATA_PARTITION_ID}.dataservices.energy" if DATA_PARTITION_ID else "partition.dataservices.energy"

# Sensible defaults for the "Create Dataspace" form (can be overridden in env)
DEFAULT_LEGAL_TAG: str = os.getenv("DEFAULT_LEGAL_TAG", f"{DATA_PARTITION_ID}-RDDMS-Legal-Tag" if DATA_PARTITION_ID else "dp1-RDDMS-Legal-Tag")

_default_owners = os.getenv("DEFAULT_OWNERS", f"data.default.owners@{_partition_suffix()}")
DEFAULT_OWNERS: List[str] = [x.strip() for x in _default_owners.split(",") if x.strip()]

_default_viewers = os.getenv("DEFAULT_VIEWERS", f"data.default.viewers@{_partition_suffix()}")
DEFAULT_VIEWERS: List[str] = [x.strip() for x in _default_viewers.split(",") if x.strip()]

_default_countries = os.getenv("DEFAULT_COUNTRIES", "US")
DEFAULT_COUNTRIES: List[str] = [x.strip() for x in _default_countries.split(",") if x.strip()]

# ----------------------------------------------------------------------
# HTTP utils
# ----------------------------------------------------------------------

def headers(access_token: str) -> Dict[str, str]:
    if not DATA_PARTITION_ID:
        log.warning("DATA_PARTITION_ID env var is not set; calls may fail")
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "data-partition-id": DATA_PARTITION_ID,
    }

# ----------------------------------------------------------------------
# Dataspaces
# ----------------------------------------------------------------------

async def list_dataspaces(access_token: str) -> List[Dict[str, Any]]:
    """GET /api/reservoir-ddms/v2/dataspaces"""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or []

async def create_dataspace(
    access_token: str,
    path: str,
    *,
    legal_tag: str,
    owners: List[str],
    viewers: List[str],
    countries: List[str],
    extra_custom: Optional[Dict[str, Any]] = None,
) -> Any:
    """POST /api/reservoir-ddms/v2/dataspaces"""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces"

    custom: Dict[str, Any] = {
        "legaltags": [legal_tag],
        "otherRelevantDataCountries": countries,
        "viewers": viewers,
        "owners": owners,
    }
    if extra_custom:
        # Do not let extra keys override reserved compliance ACL fields
        for k in ("legaltags", "otherRelevantDataCountries", "viewers", "owners"):
            extra_custom.pop(k, None)
        custom.update(extra_custom)

    payload = [
        {
            "DataspaceId": path,
            "Path": path,
            "CustomData": custom,
        }
    ]

    hdr = headers(access_token)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=hdr, json=payload)

    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
        log.error(
            "Dataspace create failed (%s) corr=%s\nURL=%s\nHeaders=%s\nPayload=%s\nResponseHeaders=%s\nBody=%s",
            r.status_code, corr, url, hdr, json.dumps(payload, indent=2),
            dict(r.headers), r.text
        )
        raise
    return r.json()

# ----------------------------------------------------------------------
# Types & resources
# ----------------------------------------------------------------------

async def list_types(access_token: str, ds_enc: str) -> List[Dict[str, Any]]:
    """GET /dataspaces/{dataspaceId}/resources -> list of {'name','count'}"""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or []

async def list_resources(access_token: str, ds_enc: str, typ: str) -> List[Dict[str, Any]]:
    """GET /dataspaces/{dataspaceId}/resources/{dataObjectType}"""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources/{typ}"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or []

async def get_resource(
    access_token: str,
    ds_enc: str,
    typ: str,
    uuid: str,
    *,
    include_refs: bool = False,  # reserved for future expansion
) -> Dict[str, Any]:
    """GET /dataspaces/{dataspaceId}/resources/{dataObjectType}/{guid}"""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources/{typ}/{uuid}"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or {}

async def list_arrays(access_token: str, ds_enc: str, typ: str, uuid: str) -> List[Dict[str, Any]]:
    """GET arrays metadata list for an object."""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources/{typ}/{uuid}/arrays"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or []

async def read_array(
    access_token: str,
    ds_enc: str,
    typ: str,
    uuid: str,
    *,
    path_in_resource: str,
) -> Dict[str, Any]:
    """GET content of an array."""
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{ds_enc}/resources/{typ}/{uuid}/arrays/{path_in_resource}"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=headers(access_token))
        r.raise_for_status()
        return r.json() or {}

# ----------------------------------------------------------------------
# Helpers for UI features
# ----------------------------------------------------------------------

def extract_refs(obj: Dict[str, Any]) -> List[Dict[str, str]]:
    """Very lightweight scan for DataObjectReference-like dicts."""
    edges: List[Dict[str, str]] = []

    def _walk(x: Any):
        if isinstance(x, dict):
            ct = x.get("ContentType")
            uid = x.get("UUID") or x.get("Uuid")
            if ct and uid:
                edges.append({"contentType": ct, "uuid": str(uid)})
            for v in x.values():
                _walk(v)
        elif isinstance(x, list):
            for v in x:
                _walk(v)

    _walk(obj)
    return edges

def extract_grid2d_geometry(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract minimal visualization metadata from a Grid2dRepresentation."""
    if not (obj.get("$type", "") or "").endswith("Grid2dRepresentation"):
        return None
    try:
        patch = obj["Grid2dPatch"]
        fast = int(patch["FastestAxisCount"])
        slow = int(patch["SlowestAxisCount"])
        geom = patch["Geometry"]
        pts = geom["Points"]
        origin = pts["Origin"]
        offsets = pts["Offset"]
        u = offsets[0]
        v = offsets[1]
        return {
            "fast": fast,
            "slow": slow,
            "origin": {
                "x": origin.get("Coordinate1", 0.0),
                "y": origin.get("Coordinate2", 0.0),
                "z": origin.get("Coordinate3", 0.0),
            },
            "u": {
                "dx": (u.get("Offset") or {}).get("Coordinate1", 0.0),
                "dy": (u.get("Offset") or {}).get("Coordinate2", 0.0),
                "spacing": ((u.get("Spacing") or {}).get("Value", 1.0)),
            },
            "v": {
                "dx": (v.get("Offset") or {}).get("Coordinate1", 0.0),
                "dy": (v.get("Offset") or {}).get("Coordinate2", 0.0),
                "spacing": ((v.get("Spacing") or {}).get("Value", 1.0)),
            },
        }
    except Exception:
        return None

async def delete_dataspace(access_token: str, path: str) -> None:
    """
    DELETE /api/reservoir-ddms/v2/dataspaces/{dataspaceId}
    """
    enc = urllib.parse.quote(path, safe="")
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{enc}"
    hdr = headers(access_token)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.delete(url, headers=hdr)
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
        log.error("Dataspace delete failed (%s) corr=%s path=%s body=%s",
                  r.status_code, corr, path, r.text)
        raise

# --- add these helpers to app/osdu.py ---

async def lock_dataspace(access_token: str, path: str) -> None:
    """
    POST /api/reservoir-ddms/v2/dataspaces/{dataspaceId}/lock
    """
    enc = urllib.parse.quote(path, safe="")
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{enc}/lock"
    hdr = headers(access_token)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=hdr)
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
        log.error("Dataspace lock failed (%s) corr=%s path=%s body=%s",
                  r.status_code, corr, path, r.text)
        raise

async def unlock_dataspace(access_token: str, path: str) -> None:
    """
    DELETE /api/reservoir-ddms/v2/dataspaces/{dataspaceId}/lock
    """
    enc = urllib.parse.quote(path, safe="")
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{enc}/lock"
    hdr = headers(access_token)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.delete(url, headers=hdr)
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
        log.error("Dataspace unlock failed (%s) corr=%s path=%s body=%s",
                  r.status_code, corr, path, r.text)
        raise

async def delete_dataspace(access_token: str, path: str) -> None:
    """
    DELETE /api/reservoir-ddms/v2/dataspaces/{dataspaceId}
    """
    enc = urllib.parse.quote(path, safe="")
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/dataspaces/{enc}"
    hdr = headers(access_token)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.delete(url, headers=hdr)
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
        log.error("Dataspace delete failed (%s) corr=%s path=%s body=%s",
                  r.status_code, corr, path, r.text)
        raise

def _dataspace_uri(path: str) -> str:
    # Canonical form seen in responses: eml:///dataspace('demo/Volve')
    return f"eml:///dataspace('{path}')"

async def build_manifest(
    access_token: str,
    path: str,
    *,
    legal_tag: str | None = None,
    owners: list[str] | None = None,
    viewers: list[str] | None = None,
    countries: list[str] | None = None,
    create_missing_refs: bool = True,
) -> dict:
    """
    POST /api/reservoir-ddms/v2/manifests/build
    Body typically includes: uris[], acl{}, legal{}, createMissingReferences
    """
    url = f"https://{OSDU_BASE_URL}/api/reservoir-ddms/v2/manifests/build"
    hdr = headers(access_token)

    # Use sensible defaults if not provided
    legal_tag = legal_tag or DEFAULT_LEGAL_TAG
    owners = owners or DEFAULT_OWNERS
    viewers = viewers or DEFAULT_VIEWERS
    countries = countries or DEFAULT_COUNTRIES

    body = {
        "uris": [ _dataspace_uri(path) ],
        "acl": {
            "owners": owners,
            "viewers": viewers,
        },
        "legal": {
            "legaltags": [legal_tag],
            "otherRelevantDataCountries": countries,
        },
        "createMissingReferences": bool(create_missing_refs),
    }

    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(url, headers=hdr, json=body)
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        corr = r.headers.get("x-correlation-id") or r.headers.get("x-request-id")
        log.error("Build manifest failed (%s) corr=%s path=%s body=%s",
                  r.status_code, corr, path, r.text)
        raise
    return r.json() or {}
>>>>>>> abb6fabfaa4de9d23e2761584cfe98385c375ea4
