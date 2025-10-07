from __future__ import annotations
import os
import logging
from typing import Any, Dict, List, Optional

import httpx

log = logging.getLogger("rddms-admin")
INFO = log.info
ERROR = log.error

# --- Configuration
OSDU_BASE_URL = os.getenv("OSDU_BASE_URL", "equinordev.energy.azure.com")
DATA_PARTITION_ID = os.getenv("DATA_PARTITION_ID", "data")
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID", "")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "")
AZURE_SCOPE = os.getenv("AZURE_SCOPE", "openid offline_access")

OSDU_BASE = f"https://{OSDU_BASE_URL}"
RDDMS_REST = f"{OSDU_BASE}/api/reservoir-ddms/v2"
LEGAL_API = f"{OSDU_BASE}/api/legal/v1"
ENT_API = f"{OSDU_BASE}/api/entitlements/v2"

DEFAULT_LEGAL_TAG = os.getenv(
    "DEFAULT_LEGAL_TAG", f"{DATA_PARTITION_ID}-equinor-private-default"
)
DEFAULT_COUNTRIES = [
    c.strip()
    for c in os.getenv("DEFAULT_OTHER_RELEVANT_DATA_COUNTRIES", "NO").split(",")
    if c.strip()
]
DEFAULT_OWNERS = [os.getenv("DEFAULT_OWNERS", f"data.default.owners@{DATA_PARTITION_ID}.dataservices.energy")]
DEFAULT_VIEWERS = [os.getenv("DEFAULT_VIEWERS", f"data.default.viewers@{DATA_PARTITION_ID}.dataservices.energy")]

# --- Headers
def headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "data-partition-id": DATA_PARTITION_ID,
        "content-type": "application/json",
        "accept": "application/json",
    }

# --- Generic API request
async def api_request(
    method: str,
    url: str,
    access_token: str,
    *,
    params=None,
    json_body=None,
    data=None,
    timeout=120,
) -> httpx.Response:
    hdr = headers(access_token)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.request(
            method, url, headers=hdr, params=params, json=json_body, data=data
        )
    if resp.status_code >= 400:
        ERROR("API %s %s failed: %s %s", method, url, resp.status_code, resp.text[:400])
    return resp

# --- LegalTag
async def ensure_legal_tag(
    access_token: str,
    name: str = DEFAULT_LEGAL_TAG,
    countries: List[str] = DEFAULT_COUNTRIES,
) -> None:
    url = f"{LEGAL_API}/legaltags/{name}"
    r = await api_request("GET", url, access_token)
    if r.status_code == 200:
        return

    payload = {
        "name": name,
        "properties": {
            "countryOfOrigin": countries,
            "contractId": "na",
            "dataType": "RESERVOIR",
            "originator": "admin-ui",
            "securityClassification": "PRIVATE",
            "personalData": "NO",
            "exportClassification": "EAR99",
        },
    }
    r2 = await api_request("POST", f"{LEGAL_API}/legaltags", access_token, json_body=payload)
    r2.raise_for_status()

# --- Dataspaces
async def list_dataspaces(access_token: str) -> List[Dict[str, Any]]:
    r = await api_request("GET", f"{RDDMS_REST}/dataspaces", access_token)
    r.raise_for_status()
    js = r.json() or []
    return js

async def create_dataspace(
    access_token: str,
    path: str,
    *,
    legal_tag: str = DEFAULT_LEGAL_TAG,
    owners: List[str] = DEFAULT_OWNERS,
    viewers: List[str] = DEFAULT_VIEWERS,
    countries: List[str] = DEFAULT_COUNTRIES,
) -> Dict[str, Any]:
    await ensure_legal_tag(access_token, legal_tag, countries)
    payload = {
        "path": path,
        "customData": {
            "legaltags": [legal_tag],
            "owners": owners,
            "viewers": viewers,
            "otherRelevantDataCountries": countries,
            "locked": "false",
            "read-only": "false",
        },
    }
    r = await api_request("POST", f"{RDDMS_REST}/dataspaces", access_token, json_body=payload)
    r.raise_for_status()
    return r.json()

async def delete_dataspace(access_token: str, path: str) -> None:
    r = await api_request("DELETE", f"{RDDMS_REST}/dataspaces/{path}", access_token)
    if r.status_code not in (200, 204, 404):
        r.raise_for_status()

# --- Resources
async def list_types(access_token: str, dataspace_enc: str) -> List[Dict[str, Any]]:
    r = await api_request("GET", f"{RDDMS_REST}/dataspaces/{dataspace_enc}/resources", access_token)
    r.raise_for_status()
    return r.json() or []

async def list_resources(access_token: str, dataspace_enc: str, resqml_type: str) -> List[Dict[str, Any]]:
    r = await api_request("GET", f"{RDDMS_REST}/dataspaces/{dataspace_enc}/resources/{resqml_type}", access_token)
    r.raise_for_status()
    return r.json() or []

async def get_resource(
    access_token: str,
    dataspace_enc: str,
    resqml_type: str,
    uuid: str,
    *,
    include_refs: bool = True,
) -> Dict[str, Any]:
    params = {
        "$format": "json",
        "arrayMetadata": "false",
        "arrayValues": "false",
        "referencedContent": "true" if include_refs else "false",
    }
    r = await api_request(
        "GET",
        f"{RDDMS_REST}/dataspaces/{dataspace_enc}/resources/{resqml_type}/{uuid}",
        access_token,
        params=params,
    )
    r.raise_for_status()
    js = r.json()
    return js[0] if isinstance(js, list) and js else js

async def delete_resource(access_token: str, dataspace_enc: str, resqml_type: str, uuid: str) -> None:
    r = await api_request("DELETE", f"{RDDMS_REST}/dataspaces/{dataspace_enc}/resources/{resqml_type}/{uuid}", access_token)
    if r.status_code not in (200, 204, 404):
        r.raise_for_status()

async def create_resource(access_token: str, dataspace_enc: str, resqml_type: str, body: Dict[str, Any]) -> Dict[str, Any]:
    r = await api_request(
        "POST",
        f"{RDDMS_REST}/dataspaces/{dataspace_enc}/resources/{resqml_type}",
        access_token,
        json_body=body,
    )
    r.raise_for_status()
    return r.json() if r.content else {}

# --- Arrays
async def list_arrays(access_token: str, dataspace_enc: str, resqml_type: str, uuid: str) -> List[dict]:
    params = {"$format": "json", "arrayMetadata": "false", "arrayValues": "false", "referencedContent": "true"}
    r = await api_request(
        "GET",
        f"{RDDMS_REST}/dataspaces/{dataspace_enc}/resources/{resqml_type}/{uuid}/arrays",
        access_token,
        params=params,
    )
    r.raise_for_status()
    return r.json() or []

async def read_array(access_token: str, dataspace_enc: str, resqml_type: str, uuid: str, path_in_resource: str) -> dict:
    from urllib.parse import quote
    p = quote(path_in_resource, safe="")
    r = await api_request(
        "GET",
        f"{RDDMS_REST}/dataspaces/{dataspace_enc}/resources/{resqml_type}/{uuid}/arrays/{p}",
        access_token,
        params={"format": "json"},
    )
    r.raise_for_status()
    return r.json()

# --- Extract references
def extract_refs(obj: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    edges = {"sources": [], "targets": []}

    def _collect(d: Any):
        if isinstance(d, dict):
            ct = d.get("ContentType") or d.get("contentType")
            uuid = d.get("UUID") or d.get("Uuid") or d.get("uuid")
            title = d.get("Title") or d.get("title")
            if ct and uuid:
                edges["targets"].append({"contentType": ct, "uuid": uuid, "title": title or ""})
            for v in d.values():
                _collect(v)
        elif isinstance(d, list):
            for v in d:
                _collect(v)

    _collect(obj)
    return edges

# --- Geometry helper for Grid2d
def extract_grid2d_geometry(obj: dict) -> dict | None:
    try:
        gp = obj.get("Grid2dPatch") or {}
        geom = gp.get("Geometry") or {}
        points = geom.get("Points") or {}
        lat = None
        if points.get("$type") == "resqml20.Point3dLatticeArray":
            lat = points
        elif points.get("$type") == "resqml20.Point3dZValueArray":
            sg = points.get("SupportingGeometry") or {}
            if isinstance(sg, dict) and sg.get("$type") == "resqml20.Point3dLatticeArray":
                lat = sg
        if not lat:
            return None
        origin = [lat["Origin"]["Coordinate1"], lat["Origin"]["Coordinate2"]]
        offs = lat.get("Offset", [])
        v1 = [offs[0]["Offset"]["Coordinate1"], offs[0]["Offset"]["Coordinate2"]]
        v2 = [offs[1]["Offset"]["Coordinate1"], offs[1]["Offset"]["Coordinate2"]]
        s1 = (offs[0].get("Spacing") or {}).get("Value", 1.0)
        s2 = (offs[1].get("Spacing") or {}).get("Value", 1.0)
        u = [v1[0] * s1, v1[1] * s1]
        v = [v2[0] * s2, v2[1] * s2]
        size = [gp.get("FastestAxisCount"), gp.get("SlowestAxisCount")]
        return {"origin": origin, "u": u, "v": v, "size": size}
    except Exception:
        return None