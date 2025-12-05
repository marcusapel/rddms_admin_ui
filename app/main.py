from __future__ import annotations
import os
import urllib.parse
import logging
import json
import numpy as np
import httpx
from typing import List, Dict, Any, Optional, Tuple
from httpx import HTTPStatusError
from fastapi import FastAPI, Request, Form, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import Response

from app.osdu import submit_workflow_run
from app.ingest_router import router as ingest_router

from . import osdu
from .auth import (
    router as auth_router,
    tokens_from_env,
)

# ───────────────────────────────────────────────────────────────────────────────
# App setup & logging
# ───────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("rddms-admin")

app = FastAPI(title="RDDMS Admin")

# Security headers & cache hardening
@app.middleware("http")
async def no_transform_headers(request: Request, call_next):
    resp: Response = await call_next(request)
    resp.headers.setdefault("Cache-Control", "no-store, no-transform")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    return resp

# Auth: server-side refresh-token minting (no cookies)
@app.middleware("http")
async def inject_access_token(request: Request, call_next):
    """
    Mint a fresh access_token from REFRESH_TOKEN and attach to request.state.
    Fails fast with 401 if unavailable.
    """
    try:
        tokens = await tokens_from_env()
        if not tokens or not tokens.get("access_token"):
            log.error("Auth failed: missing/invalid refresh_token")
            return JSONResponse({"error": "Authentication failed: missing/invalid refresh_token"}, status_code=401)
        request.state.access_token = tokens["access_token"]
    except Exception as e:
        log.error("Failed to mint access token: %s", e)
        return JSONResponse({"error": f"Authentication failed: {e}"}, status_code=401)
    return await call_next(request)

# Attach routers & static
app.include_router(auth_router)  # keeps /auth diagnostics
app.include_router(ingest_router, prefix="/api")
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)

# Log routes at startup (helps when a route goes missing)
log.info("Routes registered: %s", [getattr(r, "path", str(r)) for r in app.routes])

# ───────────────────────────────────────────────────────────────────────────────
# Utilities
# ───────────────────────────────────────────────────────────────────────────────
def _access_token(request: Request) -> str:
    at = getattr(request.state, "access_token", None)
    if not at:
        raise HTTPException(401, "Authentication failed")
    return at


def _normalize_volumes(data_block: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize OSDU ColumnBasedTable in data_block['Volumes'] to a structure:
      {
        "KeyColumns": [ {ColumnName, ColumnRole, ValueType, ...}, ... ],
        "Columns":    [ {ColumnName, ColumnRole, ValueType, ...}, ... ],
        "ColumnValues": { "<ColumnName>": [v0, v1, ...], ... }
      }

    Handles cases where ColumnValues may arrive as a dict or as a list of objects.
    Leaves other shapes untouched (best-effort).
    """
    vol = (data_block or {}).get("Volumes", {}) or {}

    key_cols = vol.get("KeyColumns", []) or []
    value_cols = vol.get("Columns", []) or []

    raw_vals = vol.get("ColumnValues", {}) or {}

    # Prefer dict[str, list]; attempt best-effort normalization if it's a list
    if isinstance(raw_vals, dict):
        col_values = raw_vals
    elif isinstance(raw_vals, list):
        # Case: list of dicts like {"ColumnName": "...", "Values": [...]}
        if raw_vals and all(isinstance(x, dict) for x in raw_vals):
            out: Dict[str, List[Any]] = {}
            for x in raw_vals:
                name = x.get("ColumnName") or x.get("name")
                vals = (
                    x.get("Values")
                    or x.get("values")
                    or x.get("Data")
                    or x.get("data")
                    or []
                )
                if name:
                    out[name] = vals if isinstance(vals, list) else [vals]
            col_values = out
        else:
            # Unknown shape; keep as-is (template has a fallback)
            col_values = raw_vals
    else:
        col_values = raw_vals

    return {
        "KeyColumns": key_cols,
        "Columns": value_cols,
        "ColumnValues": col_values,
    }

# ───────────────────────────────────────────────────────────────────────────────
# Pages & actions
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse, summary="Home: list dataspaces")
async def home(request: Request):
    try:
        at = _access_token(request)
        dataspaces = await osdu.list_dataspaces(at)
    except Exception as e:
        log.warning("List dataspaces failed: %s", e)
        dataspaces = []
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "view": "home",
            "dataspaces": dataspaces,
            # Defaults for the "Create Dataspace" form (prefilled values)
            "ds_default": os.getenv("DEFAULT_DATASPACE", ""),
            "default_legal_tag": osdu.DEFAULT_LEGAL_TAG,
            "default_owners": ",".join(osdu.DEFAULT_OWNERS),
            "default_viewers": ",".join(osdu.DEFAULT_VIEWERS),
            "default_countries": ",".join(osdu.DEFAULT_COUNTRIES),
        },
    )


@app.post("/dataspaces/create", summary="Create a dataspace with default legal/ACL")
async def dataspaces_create(
    request: Request,
    path: str = Form(...),
    legal: str = Form(osdu.DEFAULT_LEGAL_TAG),
    owners: str = Form(",".join(osdu.DEFAULT_OWNERS)),
    viewers: str = Form(",".join(osdu.DEFAULT_VIEWERS)),
    countries: str = Form(",".join(osdu.DEFAULT_COUNTRIES)),
    custom_json: str = Form("", description="Optional JSON to merge into CustomData"),
):
    at = _access_token(request)

    # Parse optional JSON block
    extra_custom: Dict[str, Any] = {}
    if custom_json and custom_json.strip():
        try:
            extra_custom = json.loads(custom_json)
            if not isinstance(extra_custom, dict):
                raise ValueError("Custom data must be a JSON object")
        except Exception as ex:
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "view": "home",
                    "dataspaces": [],
                    "ds_default": os.getenv("DEFAULT_DATASPACE", ""),
                    "default_legal_tag": osdu.DEFAULT_LEGAL_TAG,
                    "default_owners": ",".join(osdu.DEFAULT_OWNERS),
                    "default_viewers": ",".join(osdu.DEFAULT_VIEWERS),
                    "default_countries": ",".join(osdu.DEFAULT_COUNTRIES),
                    "error": "Invalid custom JSON",
                    "error_detail": str(ex),
                },
                status_code=400,
            )

    try:
        await osdu.create_dataspace(
            at,
            path,
            legal_tag=legal,
            owners=[x.strip() for x in owners.split(",") if x.strip()],
            viewers=[x.strip() for x in viewers.split(",") if x.strip()],
            countries=[x.strip() for x in countries.split(",") if x.strip()],
            extra_custom=extra_custom,
        )
    except HTTPStatusError as e:
        r = e.response
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "view": "home",
                "dataspaces": [],
                "ds_default": os.getenv("DEFAULT_DATASPACE", ""),
                "default_legal_tag": osdu.DEFAULT_LEGAL_TAG,
                "default_owners": ",".join(osdu.DEFAULT_OWNERS),
                "default_viewers": ",".join(osdu.DEFAULT_VIEWERS),
                "default_countries": ",".join(osdu.DEFAULT_COUNTRIES),
                "error": f"Create failed: {r.status_code} {r.reason_phrase}",
                "error_detail": (r.text[:2000] if r.text else ""),
            },
            status_code=400,
        )

    return RedirectResponse(url="/", status_code=302)


@app.get(
    "/d/{ds:path}/t/{typ}/{uuid}/arrays/read",
    summary="Read a RESQML array (JSON) and return stats",
)
async def array_read(request: Request, ds: str, typ: str, uuid: str, path: str):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    js = await osdu.read_array(at, enc, typ, uuid, path_in_resource=path)
    dims = js["data"]["dimensions"]
    values = np.array(js["data"]["data"], dtype=np.float32)
    n = int(np.prod(dims))
    if values.size != n:
        values = values[:n]
    z = values.reshape(dims)
    stats = {
        "min": float(np.nanmin(z)),
        "max": float(np.nanmax(z)),
        "mean": float(np.nanmean(z)),
        "std": float(np.nanstd(z)),
        "dims": dims,
    }
    return JSONResponse({"stats": stats})

# ───────────────────────────────────────────────────────────────────────────────
# Guided Create Forms (server endpoints)
# ───────────────────────────────────────────────────────────────────────────────
@app.post("/d/{ds:path}/new/property-kind", summary="Create resqml20.obj_PropertyKind")
async def create_property_kind(
    request: Request,
    ds: str,
    title: str = Form(...),
    quantity_class: str = Form("discrete"),
    naming_system: str = Form("http://example.com"),
):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    body = {
        "$type": "resqml20.obj_PropertyKind",
        "SchemaVersion": "2.0",
        "Uuid": "",
        "Citation": {"$type": "eml20.Citation", "Title": title},
        "NamingSystem": naming_system,
        "QuantityClass": quantity_class,
    }
    res = await osdu.create_resource(at, enc, "resqml20.obj_PropertyKind", body) if hasattr(osdu, "create_resource") else {"warning": "create_resource not implemented"}
    return JSONResponse({"status": "ok", "created": res})


@app.post("/d/{ds:path}/new/string-lookup", summary="Create resqml20.obj_StringTableLookup")
async def create_string_lookup(
    request: Request, ds: str, title: str = Form(...), entries: str = Form("A,B,C")
):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    strings = [{"Index": i, "Value": v.strip()} for i, v in enumerate(entries.split(","))]
    body = {
        "$type": "resqml20.obj_StringTableLookup",
        "SchemaVersion": "2.0",
        "Uuid": "",
        "Citation": {"$type": "eml20.Citation", "Title": title},
        "Strings": strings,
    }
    res = await osdu.create_resource(at, enc, "resqml20.obj_StringTableLookup", body) if hasattr(osdu, "create_resource") else {"warning": "create_resource not implemented"}
    return JSONResponse({"status": "ok", "created": res})


@app.post("/d/{ds:path}/new/local-crs", summary="Create resqml20.obj_LocalDepth3dCrs (minimal)")
async def create_local_crs(request: Request, ds: str, title: str = Form("Local Depth CRS")):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    body = {
        "$type": "resqml20.obj_LocalDepth3dCrs",
        "SchemaVersion": "2.0",
        "Uuid": "",
        "Citation": {"$type": "eml20.Citation", "Title": title},
        "VerticalAxisUnit": {"$type": "eml20.Uom", "Unit": "m"},
        "ProjectedAxisUom": {"$type": "eml20.Uom", "Unit": "m"},
    }
    res = await osdu.create_resource(at, enc, "resqml20.obj_LocalDepth3dCrs", body) if hasattr(osdu, "create_resource") else {"warning": "create_resource not implemented"}
    return JSONResponse({"status": "ok", "created": res})


@app.post("/d/{ds:path}/new/grid2d", summary="Create resqml20.obj_Grid2dRepresentation (lattice)")
async def create_grid2d(
    request: Request,
    ds: str,
    title: str = Form("Horizon"),
    crs_uuid: str = Form(...),
    n_fast: int = Form(350),
    n_slow: int = Form(550),
    origin_x: float = Form(...),
    origin_y: float = Form(...),
    u_x: float = Form(...),
    u_y: float = Form(...),
    v_x: float = Form(...),
    v_y: float = Form(...),
    u_spacing: float = Form(1.0),
    v_spacing: float = Form(1.0),
):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    lattice = {
        "$type": "resqml20.Point3dLatticeArray",
        "Origin": {"$type": "resqml20.Point3d", "Coordinate1": origin_x, "Coordinate2": origin_y, "Coordinate3": 0.0},
        "Offset": [
            {"Offset": {"Coordinate1": u_x, "Coordinate2": u_y, "Coordinate3": 0.0}, "Spacing": {"$type": "eml20.LengthMeasure", "Value": u_spacing}},
            {"Offset": {"Coordinate1": v_x, "Coordinate2": v_y, "Coordinate3": 0.0}, "Spacing": {"$type": "eml20.LengthMeasure", "Value": v_spacing}},
        ],
    }
    body = {
        "$type": "resqml20.obj_Grid2dRepresentation",
        "SchemaVersion": "2.0",
        "Uuid": "",
        "Citation": {"$type": "eml20.Citation", "Title": title},
        "SurfaceRole": "map",
        "Grid2dPatch": {
            "$type": "resqml20.Grid2dPatch",
            "PatchIndex": 0,
            "FastestAxisCount": n_fast,
            "SlowestAxisCount": n_slow,
            "Geometry": {
                "$type": "resqml20.PointGeometry",
                "LocalCrs": {
                    "$type": "eml20.DataObjectReference",
                    "ContentType": "application/x-resqml+xml;version=2.0;type=obj_LocalDepth3dCrs",
                    "UUID": crs_uuid,
                },
                "Points": lattice,
            },
        },
    }
    res = await osdu.create_resource(at, enc, "resqml20.obj_Grid2dRepresentation", body) if hasattr(osdu, "create_resource") else {"warning": "create_resource not implemented"}
    return JSONResponse({"status": "ok", "created": res})

# ───────────────────────────────────────────────────────────────────────────────
# Search (OSDU search v2) — with robust debug logs & graceful HTML
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/search", response_class=HTMLResponse, summary="Search form (OSDU search v2)")
async def search_page(request: Request):
    # Pre-fill demo values
    return templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "kind": "osdu:wks:work-product-component--ReservoirEstimatedVolumes:1.1.0",
            "q": "*",
            "limit": 5,
            "returnedFields": "id,kind,version",
        },
    )


@app.post("/search/run", response_class=HTMLResponse)
async def search_run(
    request: Request,
    kind: str = Form("osdu:wks:work-product-component--ReservoirEstimatedVolumes:1.1.0"),
    query: str = Form("*"),
    limit: int = Form(5),
):
    at = _access_token(request)

    search_url = f"https://{osdu.OSDU_BASE_URL}/api/search/v2/query"
    storage_url = f"https://{osdu.OSDU_BASE_URL}/api/storage/v2/records"
    hdr = osdu.headers(at)
    payload = {
        "kind": kind,
        "query": query,
        "limit": int(limit),
        "returnedFields": ["id", "kind", "version"],  # minimal
        "trackTotalCount": True,
    }

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            # Search
            r = await client.post(search_url, headers=hdr, json=payload)
            r.raise_for_status()
            res = r.json()
            log.info("[SEARCH] Status=%d, total=%d", r.status_code, len(res.get("results", [])))

            enriched_results: List[Dict[str, Any]] = []
            for rec in res.get("results", []):
                rid = rec.get("id")
                if not rid:
                    continue
                try:
                    r_full = await client.get(f"{storage_url}/{rid}", headers=hdr)
                    if r_full.status_code == 200:
                        full = r_full.json()
                        data_block = full.get("data", {}) or {}
                        enriched_results.append({
                            "id": full.get("id"),
                            "kind": full.get("kind"),
                            "version": full.get("version"),
                            "data": data_block,
                            "ancestry_parents": data_block.get("ancestry", {}).get("parents", []),
                            "ancestry_children": data_block.get("ancestry", {}).get("children", []),
                            # Normalize Volumes for HTML rendering
                            "volumes": _normalize_volumes(data_block),
                        })
                    else:
                        log.warning("[SEARCH] Full record fetch failed for %s: %d", rid, r_full.status_code)
                except Exception as e:
                    log.warning("[SEARCH] Exception fetching %s: %s", rid, e)
    except Exception:
        log.exception("[SEARCH] Unexpected error")
        return templates.TemplateResponse(
            "search.html",
            {"request": request, "error": "Unexpected error", "error_detail": "See logs"},
            status_code=500,
        )

    return templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "results": {"results": enriched_results, "totalCount": len(enriched_results)},
            "kind": kind,
            "q": query,
            "limit": limit,
        },
    )


@app.get("/search/view/{record_id}", response_class=HTMLResponse)
async def view_record(request: Request, record_id: str):
    at = _access_token(request)
    storage_url = f"https://{osdu.OSDU_BASE_URL}/api/storage/v2/records/{record_id}"
    hdr = osdu.headers(at)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(storage_url, headers=hdr)
        r.raise_for_status()
        full = r.json()
        data_block = full.get("data", {}) or {}
        volumes = _normalize_volumes(data_block)
        return templates.TemplateResponse(
            "record.html",
            {
                "request": request,
                "record": full,
                "volumes": volumes,
            },
        )

# ───────────────────────────────────────────────────────────────────────────────
# KEYS page: dataspace -> type -> object (kept for convenience)
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/keys", response_class=HTMLResponse)
async def keys_page(request: Request):
    prefill_ds = []
    try:
        at = _access_token(request)
        rows = await osdu.list_dataspaces(at)
        prefill_ds = [{"path": x.get("path", ""), "uri": x.get("uri", "")} for x in (rows or []) if x.get("path")]
    except Exception as e:
        log.warning("keys_page list_dataspaces failed: %s", e)
        prefill_ds = []
    return templates.TemplateResponse(
        "keys.html",
        {"request": request, "prefill_ds": prefill_ds},
        media_type="text/html",
    )


@app.get("/keys/dataspaces.json")
async def keys_dataspaces(request: Request):
    at = _access_token(request)
    try:
        rows = await osdu.list_dataspaces(at)
    except Exception as e:
        log.warning("keys_dataspaces failed: %s", e)
        rows = []
    items = [{"path": x.get("path"), "uri": x.get("uri")} for x in rows if x.get("path")]
    return JSONResponse({"items": items})


@app.get("/keys/types.json")
async def keys_types(
    request: Request,
    ds: str = Query(..., description="Dataspace path"),
    source: str = Query("live", description="'live' (Rddms) or 'catalog' (curated)"),
):
    at = _access_token(request)
    items: List[Dict[str, Any]] = []
    if source == "live":
        enc = urllib.parse.quote(ds, safe="")
        try:
            rows = await osdu.list_types(at, enc)
        except Exception as e:
            log.warning("keys_types list_types failed: %s", e)
            rows = []
        for r in rows or []:
            name = r.get("name") if isinstance(r, dict) else r
            count = r.get("count") if isinstance(r, dict) else None
            if name:
                items.append({"name": name, "count": count})
    else:
        # curated fallback list
        items = [{"name": x} for x in [
            "resqml20.obj_PropertyKind",
            "resqml20.obj_StringTableLookup",
            "resqml20.obj_LocalDepth3dCrs",
            "resqml20.obj_Grid2dRepresentation",
            "resqml20.obj_HorizonInterpretation",
            "resqml20.obj_GeneticBoundaryFeature",
            "resqml20.obj_IjkGridRepresentation",
            "resqml20.obj_ContinuousProperty",
            "resqml20.obj_CategoricalProperty",
            "resqml20.obj_DiscreteProperty",
            "resqml20.obj_OrganizationFeature",
            "resqml20.obj_TectonicBoundaryFeature",
            "resqml20.obj_Activity",
            "resqml20.obj_ActivityTemplate",
            "eml20.obj_EpcExternalPartReference",
        ]]
    return JSONResponse({"items": items})


@app.get("/keys/objects.json")
async def keys_objects(
    request: Request,
    ds: str = Query(..., description="Dataspace path"),
    typ: str = Query(..., description="resqml20.obj_* type"),
):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    rows = await osdu.list_resources(at, enc, typ)
    out = []
    for r in rows or []:
        uid = r.get("Uuid") or r.get("UUID") or r.get("uuid")
        if not uid:
            uri = r.get("uri") or ""
            if "(" in uri and ")" in uri:
                uid = uri.split("(")[-1].rstrip(")")
            else:
                uid = uri
        title = (r.get("Citation") or {}).get("Title") or r.get("name") or uid
        ct = r.get("$type") or r.get("contentType") or ""
        out.append({"uuid": uid, "title": title, "uri": r.get("uri", ""), "contentType": ct})
    return JSONResponse({"items": out})


@app.get("/keys/object.json")
async def keys_object(
    request: Request,
    ds: str = Query(...),
    typ: str = Query(...),
    uuid: str = Query(...),
):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    # Metadata (with referencedContent - reserved for future)
    obj = await osdu.get_resource(at, enc, typ, uuid, include_refs=True)

    # --- Normalize possible list shapes to a single dict for UI ---
    raw_obj = obj  # keep a copy for return (diagnostics)
    if isinstance(obj, list):
        # pick the first dict item if present; else empty dict
        obj = next((x for x in obj if isinstance(x, dict)), {}) if obj else {}
    if not isinstance(obj, dict):
        # If we still don't have a dict, downgrade gracefully
        return JSONResponse({"obj": obj, "raw": raw_obj, "edges": [], "arrays": [], "geom": None})

    # Edges
    edges = osdu.extract_refs(obj) if hasattr(osdu, "extract_refs") else []

    # Arrays
    try:
        arrays = await osdu.list_arrays(at, enc, typ, uuid)
    except Exception as e:
        log.warning("keys_object list_arrays failed: %s", e)
        arrays = []

    # Geometry (Grid2d only)
    t = (obj.get("$type") or obj.get("contentType") or "")
    is_grid2d = t.endswith("Grid2dRepresentation") or "obj_Grid2dRepresentation" in t
    geom = osdu.extract_grid2d_geometry(obj) if (is_grid2d and hasattr(osdu, "extract_grid2d_geometry")) else None

    # Normalize Volumes (if present)
    data_block = obj if isinstance(obj, dict) else {}
    volumes = _normalize_volumes(data_block)

    return JSONResponse({"obj": obj, "raw": raw_obj, "edges": edges, "arrays": arrays, "geom": geom, "volumes": volumes})

# ───────────────────────────────────────────────────────────────────────────────
# Dataspace admin endpoints (delete/lock/unlock/manifest)
# ───────────────────────────────────────────────────────────────────────────────
@app.post("/dataspaces/delete", summary="Delete a dataspace")
async def dataspaces_delete(request: Request, path: str = Form(...)):
    at = _access_token(request)
    try:
        await osdu.delete_dataspace(at, path)
    except HTTPStatusError as e:
        r = e.response
        return JSONResponse(
            {
                "status": "error",
                "code": r.status_code,
                "reason": r.reason_phrase,
                "detail": (r.text[:2000] if r.text else ""),
            },
            status_code=r.status_code or 500,
        )
    return JSONResponse({"status": "ok"})


@app.post("/dataspaces/lock", summary="Lock a dataspace")
async def dataspaces_lock(request: Request, path: str = Form(...)):
    at = _access_token(request)
    try:
        await osdu.lock_dataspace(at, path)
    except HTTPStatusError as e:
        r = e.response
        return JSONResponse(
            {"status": "error", "code": r.status_code, "reason": r.reason_phrase, "detail": (r.text[:2000] if r.text else "")},
            status_code=r.status_code or 500,
        )
    return JSONResponse({"status": "ok"})


@app.post("/dataspaces/unlock", summary="Unlock a dataspace")
async def dataspaces_unlock(request: Request, path: str = Form(...)):
    at = _access_token(request)
    try:
        await osdu.unlock_dataspace(at, path)
    except HTTPStatusError as e:
        r = e.response
        return JSONResponse(
            {"status": "error", "code": r.status_code, "reason": r.reason_phrase, "detail": (r.text[:2000] if r.text else "")},
            status_code=r.status_code or 500,
        )
    return JSONResponse({"status": "ok"})



@app.post("/dataspaces/manifest", summary="Build + Ingest OSDU manifest (server-side)")
async def dataspaces_manifest(
    request: Request,
    path: str = Form(..., description="Dataspace path, e.g. maap/volve"),
    legal: str = Form(osdu.DEFAULT_LEGAL_TAG),
    owners: str = Form(",".join(osdu.DEFAULT_OWNERS)),
    viewers: str = Form(",".join(osdu.DEFAULT_VIEWERS)),
    countries: str = Form(",".join(osdu.DEFAULT_COUNTRIES)),
    create_missing: bool = Form(True),

    # Selection strategy:
    # - If uuids provided -> use (typ, uuids) + include_refs
    # - Else -> use_all_resources=True will gather /resources/all
    use_all_resources: bool = Form(True),
    typ: str = Form("", description="resqml20.obj_* type when selecting specific UUIDs"),
    uuids: str = Form("", description="Comma-separated UUIDs when selecting specific objects"),
    include_refs: bool = Form(True, description="Include immediate references (e.g., CRS) for selected UUIDs"),

    # Optional override
    app_key: str = Form("", description="Optional override of APP_KEY"),
):
    """
    1) Gather URIs (all OR selected + refs)
    2) Build manifest (explicit URIs)
    3) Submit to Workflow (server-side)
    Returns JSON; does not render the manifest.
    """
    at = _access_token(request)

    # Validate legal tag/partition alignment (prevents the earlier 401 you saw)
    partition = osdu.DATA_PARTITION_ID or ""
    if partition and not legal.startswith(f"{partition}-"):
        return JSONResponse(
            {
                "status": "error",
                "stage": "validate",
                "detail": f"Legal tag '{legal}' must start with '{partition}-' (e.g., '{partition}-equinor-private-default').",
            },
            status_code=400,
        )

    # Resolve selection set (optional)
    selections: List[Tuple[str, str]] = []
    if uuids and uuids.strip():
        if not typ:
            return JSONResponse(
                {"status": "error", "stage": "validate", "detail": "When 'uuids' is provided, 'typ' must also be provided."},
                status_code=400,
            )
        selections = [(typ, u.strip()) for u in uuids.split(",") if u.strip()]

    ds_enc = urllib.parse.quote(path, safe="")

    # Step 1: gather URIs
    try:
        if selections:
            uris = await osdu.gather_selected_uris_with_refs(at, path, selections, include_refs=include_refs)
        elif use_all_resources:
            uris = await osdu.list_all_resource_uris(at, ds_enc)
            if not uris:
                log.warning("No resources found for dataspace '%s'; falling back to dataspace URI", path)
                uris = [osdu._dataspace_uri(path)]
        else:
            uris = [osdu._dataspace_uri(path)]
    except Exception as e:
        return JSONResponse({"status": "error", "stage": "gather_uris", "detail": str(e)}, status_code=500)

    # Step 2: build manifest with explicit URIs
    try:
        manifest = await osdu.build_manifest(
            at,
            path,
            legal_tag=legal,
            owners=[x.strip() for x in owners.split(",") if x.strip()],
            viewers=[x.strip() for x in viewers.split(",") if x.strip()],
            countries=[x.strip() for x in countries.split(",") if x.strip()],
            create_missing_refs=create_missing,
            use_all_resources=False,   # we pass explicit URIs below
            explicit_uris=uris,        # important
        )
    except HTTPStatusError as e:
        r = e.response
        return JSONResponse(
            {"status": "error", "stage": "build", "code": r.status_code, "reason": r.reason_phrase, "detail": (r.text[:2000] if r.text else "")},
            status_code=r.status_code or 500,
        )

    # Step 3: ingest manifest via Workflow
    try:
        wf_response = await submit_workflow_run(
            access_token=at,
            manifest=manifest,
            partition=osdu.DATA_PARTITION_ID or "data",
            app_key=(app_key or os.getenv("APP_KEY") or None),
            run_id=None,
        )
    except HTTPException as ex:
        return JSONResponse({"status": "error", "stage": "workflow", "detail": ex.detail}, status_code=ex.status_code)
    except Exception as ex:
        return JSONResponse({"status": "error", "stage": "workflow", "detail": str(ex)}, status_code=502)

    return JSONResponse(
        {
            "status": "ok",
            "dataspace": path,
            "uris_count": len(uris),
            "workflowResponse": wf_response,
        }
    )

=======

from __future__ import annotations
import os
import urllib.parse
import logging
import json
import numpy as np
import httpx
from typing import List, Dict, Any, Optional
from httpx import HTTPStatusError
from fastapi import FastAPI, Request, Form, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import Response

# App modules
from app.ingest_router import router as ingest_router
from . import osdu
from .auth import (
    router as auth_router,
    tokens_from_env,
)

# ───────────────────────────────────────────────────────────────────────────────
# App setup & logging
# ───────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("rddms-admin")

app = FastAPI(title="RDDMS Admin")

# Security headers & cache hardening
@app.middleware("http")
async def no_transform_headers(request: Request, call_next):
    resp: Response = await call_next(request)
    resp.headers.setdefault("Cache-Control", "no-store, no-transform")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    return resp

# Auth: server-side refresh-token minting (no cookies)
@app.middleware("http")
async def inject_access_token(request: Request, call_next):
    """
    Mint a fresh access_token from REFRESH_TOKEN and attach to request.state.
    Fails fast with 401 if unavailable.
    """
    try:
        tokens = await tokens_from_env()
        if not tokens or not tokens.get("access_token"):
            log.error("Auth failed: missing/invalid refresh_token")
            return JSONResponse({"error": "Authentication failed: missing/invalid refresh_token"}, status_code=401)
        request.state.access_token = tokens["access_token"]
    except Exception as e:
        log.error("Failed to mint access token: %s", e)
        return JSONResponse({"error": f"Authentication failed: {e}"}, status_code=401)
    return await call_next(request)

# Attach routers & static
app.include_router(auth_router)  # keeps /auth diagnostics
app.include_router(ingest_router, prefix="/api")
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)

# Log routes at startup (helps when a route goes missing)
log.info("Routes registered: %s", [getattr(r, "path", str(r)) for r in app.routes])

# ───────────────────────────────────────────────────────────────────────────────
# Utilities
# ───────────────────────────────────────────────────────────────────────────────
def _access_token(request: Request) -> str:
    at = getattr(request.state, "access_token", None)
    if not at:
        raise HTTPException(401, "Authentication failed")
    return at


def _normalize_volumes(data_block: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize OSDU ColumnBasedTable in data_block['Volumes'] to a structure:
      {
        "KeyColumns": [ {ColumnName, ColumnRole, ValueType, ...}, ... ],
        "Columns":    [ {ColumnName, ColumnRole, ValueType, ...}, ... ],
        "ColumnValues": { "<ColumnName>": [v0, v1, ...], ... }
      }

    Handles cases where ColumnValues may arrive as a dict or as a list of objects.
    Leaves other shapes untouched (best-effort).
    """
    vol = (data_block or {}).get("Volumes", {}) or {}

    key_cols = vol.get("KeyColumns", []) or []
    value_cols = vol.get("Columns", []) or []

    raw_vals = vol.get("ColumnValues", {}) or {}

    # Prefer dict[str, list]; attempt best-effort normalization if it's a list
    if isinstance(raw_vals, dict):
        col_values = raw_vals
    elif isinstance(raw_vals, list):
        # Case: list of dicts like {"ColumnName": "...", "Values": [...]}
        if raw_vals and all(isinstance(x, dict) for x in raw_vals):
            out: Dict[str, List[Any]] = {}
            for x in raw_vals:
                name = x.get("ColumnName") or x.get("name")
                vals = (
                    x.get("Values")
                    or x.get("values")
                    or x.get("Data")
                    or x.get("data")
                    or []
                )
                if name:
                    out[name] = vals if isinstance(vals, list) else [vals]
            col_values = out
        else:
            # Unknown shape; keep as-is (template has a fallback)
            col_values = raw_vals
    else:
        col_values = raw_vals

    return {
        "KeyColumns": key_cols,
        "Columns": value_cols,
        "ColumnValues": col_values,
    }

# ───────────────────────────────────────────────────────────────────────────────
# Pages & actions
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse, summary="Home: list dataspaces")
async def home(request: Request):
    try:
        at = _access_token(request)
        dataspaces = await osdu.list_dataspaces(at)
    except Exception as e:
        log.warning("List dataspaces failed: %s", e)
        dataspaces = []
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "view": "home",
            "dataspaces": dataspaces,
            # Defaults for the "Create Dataspace" form (prefilled values)
            "ds_default": os.getenv("DEFAULT_DATASPACE", ""),
            "default_legal_tag": osdu.DEFAULT_LEGAL_TAG,
            "default_owners": ",".join(osdu.DEFAULT_OWNERS),
            "default_viewers": ",".join(osdu.DEFAULT_VIEWERS),
            "default_countries": ",".join(osdu.DEFAULT_COUNTRIES),
        },
    )


@app.post("/dataspaces/create", summary="Create a dataspace with default legal/ACL")
async def dataspaces_create(
    request: Request,
    path: str = Form(...),
    legal: str = Form(osdu.DEFAULT_LEGAL_TAG),
    owners: str = Form(",".join(osdu.DEFAULT_OWNERS)),
    viewers: str = Form(",".join(osdu.DEFAULT_VIEWERS)),
    countries: str = Form(",".join(osdu.DEFAULT_COUNTRIES)),
    custom_json: str = Form("", description="Optional JSON to merge into CustomData"),
):
    at = _access_token(request)

    # Parse optional JSON block
    extra_custom: Dict[str, Any] = {}
    if custom_json and custom_json.strip():
        try:
            extra_custom = json.loads(custom_json)
            if not isinstance(extra_custom, dict):
                raise ValueError("Custom data must be a JSON object")
        except Exception as ex:
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "view": "home",
                    "dataspaces": [],
                    "ds_default": os.getenv("DEFAULT_DATASPACE", ""),
                    "default_legal_tag": osdu.DEFAULT_LEGAL_TAG,
                    "default_owners": ",".join(osdu.DEFAULT_OWNERS),
                    "default_viewers": ",".join(osdu.DEFAULT_VIEWERS),
                    "default_countries": ",".join(osdu.DEFAULT_COUNTRIES),
                    "error": "Invalid custom JSON",
                    "error_detail": str(ex),
                },
                status_code=400,
            )

    try:
        await osdu.create_dataspace(
            at,
            path,
            legal_tag=legal,
            owners=[x.strip() for x in owners.split(",") if x.strip()],
            viewers=[x.strip() for x in viewers.split(",") if x.strip()],
            countries=[x.strip() for x in countries.split(",") if x.strip()],
            extra_custom=extra_custom,
        )
    except HTTPStatusError as e:
        r = e.response
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "view": "home",
                "dataspaces": [],
                "ds_default": os.getenv("DEFAULT_DATASPACE", ""),
                "default_legal_tag": osdu.DEFAULT_LEGAL_TAG,
                "default_owners": ",".join(osdu.DEFAULT_OWNERS),
                "default_viewers": ",".join(osdu.DEFAULT_VIEWERS),
                "default_countries": ",".join(osdu.DEFAULT_COUNTRIES),
                "error": f"Create failed: {r.status_code} {r.reason_phrase}",
                "error_detail": (r.text[:2000] if r.text else ""),
            },
            status_code=400,
        )

    return RedirectResponse(url=f"/d/{urllib.parse.quote(path, safe='')}", status_code=302)


@app.get("/d/{ds:path}", response_class=HTMLResponse, summary="Dataspace view: types")
async def dataspace_view(request: Request, ds: str):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    types = await osdu.list_types(at, enc)
    return templates.TemplateResponse(
        "dataspace.html",
        {"request": request, "ds": ds, "types": types},
    )


@app.get("/d/{ds:path}/t/{typ}", response_class=HTMLResponse, summary="List resources by type")
async def type_list(request: Request, ds: str, typ: str):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    rows = await osdu.list_resources(at, enc, typ)
    return templates.TemplateResponse(
        "_fragments.html",
        {"request": request, "frag": "type_rows", "ds": ds, "typ": typ, "rows": rows},
    )


@app.get(
    "/d/{ds:path}/t/{typ}/{uuid}",
    response_class=HTMLResponse,
    summary="Resource details: metadata, references, arrays",
)
async def resource_view(request: Request, ds: str, typ: str, uuid: str):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    obj = await osdu.get_resource(at, enc, typ, uuid, include_refs=True)
    edges = osdu.extract_refs(obj)
    arrays = await osdu.list_arrays(at, enc, typ, uuid)
    geom = (
        osdu.extract_grid2d_geometry(obj)
        if (obj.get("$type", "") or "").endswith("Grid2dRepresentation")
        else None
    )
    # Normalize Volumes (if present) for downstream templates
    data_block = obj if isinstance(obj, dict) else {}
    volumes = _normalize_volumes(data_block)

    return templates.TemplateResponse(
        "resource.html",
        {
            "request": request,
            "ds": ds,
            "typ": typ,
            "uuid": uuid,
            "obj": obj,
            "edges": edges,
            "arrays": arrays,
            "geom": geom,
            "volumes": volumes,
        },
    )


@app.get(
    "/d/{ds:path}/t/{typ}/{uuid}/arrays/read",
    summary="Read a RESQML array (JSON) and return stats",
)
async def array_read(request: Request, ds: str, typ: str, uuid: str, path: str):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    js = await osdu.read_array(at, enc, typ, uuid, path_in_resource=path)
    dims = js["data"]["dimensions"]
    values = np.array(js["data"]["data"], dtype=np.float32)
    n = int(np.prod(dims))
    if values.size != n:
        values = values[:n]
    z = values.reshape(dims)
    stats = {
        "min": float(np.nanmin(z)),
        "max": float(np.nanmax(z)),
        "mean": float(np.nanmean(z)),
        "std": float(np.nanstd(z)),
        "dims": dims,
    }
    return JSONResponse({"stats": stats})

# ───────────────────────────────────────────────────────────────────────────────
# Guided Create Forms (server endpoints)
# ───────────────────────────────────────────────────────────────────────────────
@app.post("/d/{ds:path}/new/property-kind", summary="Create resqml20.obj_PropertyKind")
async def create_property_kind(
    request: Request,
    ds: str,
    title: str = Form(...),
    quantity_class: str = Form("discrete"),
    naming_system: str = Form("http://example.com"),
):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    body = {
        "$type": "resqml20.obj_PropertyKind",
        "SchemaVersion": "2.0",
        "Uuid": "",
        "Citation": {"$type": "eml20.Citation", "Title": title},
        "NamingSystem": naming_system,
        "QuantityClass": quantity_class,
    }
    res = await osdu.create_resource(at, enc, "resqml20.obj_PropertyKind", body) if hasattr(osdu, "create_resource") else {"warning": "create_resource not implemented"}
    return JSONResponse({"status": "ok", "created": res})


@app.post("/d/{ds:path}/new/string-lookup", summary="Create resqml20.obj_StringTableLookup")
async def create_string_lookup(
    request: Request, ds: str, title: str = Form(...), entries: str = Form("A,B,C")
):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    strings = [{"Index": i, "Value": v.strip()} for i, v in enumerate(entries.split(","))]
    body = {
        "$type": "resqml20.obj_StringTableLookup",
        "SchemaVersion": "2.0",
        "Uuid": "",
        "Citation": {"$type": "eml20.Citation", "Title": title},
        "Strings": strings,
    }
    res = await osdu.create_resource(at, enc, "resqml20.obj_StringTableLookup", body) if hasattr(osdu, "create_resource") else {"warning": "create_resource not implemented"}
    return JSONResponse({"status": "ok", "created": res})


@app.post("/d/{ds:path}/new/local-crs", summary="Create resqml20.obj_LocalDepth3dCrs (minimal)")
async def create_local_crs(request: Request, ds: str, title: str = Form("Local Depth CRS")):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    body = {
        "$type": "resqml20.obj_LocalDepth3dCrs",
        "SchemaVersion": "2.0",
        "Uuid": "",
        "Citation": {"$type": "eml20.Citation", "Title": title},
        "VerticalAxisUnit": {"$type": "eml20.Uom", "Unit": "m"},
        "ProjectedAxisUom": {"$type": "eml20.Uom", "Unit": "m"},
    }
    res = await osdu.create_resource(at, enc, "resqml20.obj_LocalDepth3dCrs", body) if hasattr(osdu, "create_resource") else {"warning": "create_resource not implemented"}
    return JSONResponse({"status": "ok", "created": res})


@app.post("/d/{ds:path}/new/grid2d", summary="Create resqml20.obj_Grid2dRepresentation (lattice)")
async def create_grid2d(
    request: Request,
    ds: str,
    title: str = Form("Horizon"),
    crs_uuid: str = Form(...),
    n_fast: int = Form(350),
    n_slow: int = Form(550),
    origin_x: float = Form(...),
    origin_y: float = Form(...),
    u_x: float = Form(...),
    u_y: float = Form(...),
    v_x: float = Form(...),
    v_y: float = Form(...),
    u_spacing: float = Form(1.0),
    v_spacing: float = Form(1.0),
):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    lattice = {
        "$type": "resqml20.Point3dLatticeArray",
        "Origin": {"$type": "resqml20.Point3d", "Coordinate1": origin_x, "Coordinate2": origin_y, "Coordinate3": 0.0},
        "Offset": [
            {"Offset": {"Coordinate1": u_x, "Coordinate2": u_y, "Coordinate3": 0.0}, "Spacing": {"$type": "eml20.LengthMeasure", "Value": u_spacing}},
            {"Offset": {"Coordinate1": v_x, "Coordinate2": v_y, "Coordinate3": 0.0}, "Spacing": {"$type": "eml20.LengthMeasure", "Value": v_spacing}},
        ],
    }
    body = {
        "$type": "resqml20.obj_Grid2dRepresentation",
        "SchemaVersion": "2.0",
        "Uuid": "",
        "Citation": {"$type": "eml20.Citation", "Title": title},
        "SurfaceRole": "map",
        "Grid2dPatch": {
            "$type": "resqml20.Grid2dPatch",
            "PatchIndex": 0,
            "FastestAxisCount": n_fast,
            "SlowestAxisCount": n_slow,
            "Geometry": {
                "$type": "resqml20.PointGeometry",
                "LocalCrs": {
                    "$type": "eml20.DataObjectReference",
                    "ContentType": "application/x-resqml+xml;version=2.0;type=obj_LocalDepth3dCrs",
                    "UUID": crs_uuid,
                },
                "Points": lattice,
            },
        },
    }
    res = await osdu.create_resource(at, enc, "resqml20.obj_Grid2dRepresentation", body) if hasattr(osdu, "create_resource") else {"warning": "create_resource not implemented"}
    return JSONResponse({"status": "ok", "created": res})

# ───────────────────────────────────────────────────────────────────────────────
# Search (OSDU search v2) — with robust debug logs & graceful HTML
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/search", response_class=HTMLResponse, summary="Search form (OSDU search v2)")
async def search_page(request: Request):
    # Pre-fill demo values
    return templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "kind": "osdu:wks:work-product-component--ReservoirEstimatedVolumes:1.1.0",
            "q": "*",
            "limit": 5,
            "returnedFields": "id,kind,version",
        },
    )


@app.post("/search/run", response_class=HTMLResponse)
async def search_run(
    request: Request,
    kind: str = Form("osdu:wks:work-product-component--ReservoirEstimatedVolumes:1.1.0"),
    query: str = Form("*"),
    limit: int = Form(5),
):
    at = _access_token(request)

    search_url = f"https://{osdu.OSDU_BASE_URL}/api/search/v2/query"
    storage_url = f"https://{osdu.OSDU_BASE_URL}/api/storage/v2/records"
    hdr = osdu.headers(at)
    payload = {
        "kind": kind,
        "query": query,
        "limit": int(limit),
        "returnedFields": ["id", "kind", "version"],  # minimal
        "trackTotalCount": True,
    }

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            # Search
            r = await client.post(search_url, headers=hdr, json=payload)
            r.raise_for_status()
            res = r.json()
            log.info("[SEARCH] Status=%d, total=%d", r.status_code, len(res.get("results", [])))

            enriched_results: List[Dict[str, Any]] = []
            for rec in res.get("results", []):
                rid = rec.get("id")
                if not rid:
                    continue
                try:
                    r_full = await client.get(f"{storage_url}/{rid}", headers=hdr)
                    if r_full.status_code == 200:
                        full = r_full.json()
                        data_block = full.get("data", {}) or {}
                        enriched_results.append({
                            "id": full.get("id"),
                            "kind": full.get("kind"),
                            "version": full.get("version"),
                            "data": data_block,
                            "ancestry_parents": data_block.get("ancestry", {}).get("parents", []),
                            "ancestry_children": data_block.get("ancestry", {}).get("children", []),
                            # Normalize Volumes for HTML rendering
                            "volumes": _normalize_volumes(data_block),
                        })
                    else:
                        log.warning("[SEARCH] Full record fetch failed for %s: %d", rid, r_full.status_code)
                except Exception as e:
                    log.warning("[SEARCH] Exception fetching %s: %s", rid, e)
    except Exception:
        log.exception("[SEARCH] Unexpected error")
        return templates.TemplateResponse(
            "search.html",
            {"request": request, "error": "Unexpected error", "error_detail": "See logs"},
            status_code=500,
        )

    return templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "results": {"results": enriched_results, "totalCount": len(enriched_results)},
            "kind": kind,
            "q": query,
            "limit": limit,
        },
    )


@app.get("/search/view/{record_id}", response_class=HTMLResponse)
async def view_record(request: Request, record_id: str):
    at = _access_token(request)
    storage_url = f"https://{osdu.OSDU_BASE_URL}/api/storage/v2/records/{record_id}"
    hdr = osdu.headers(at)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(storage_url, headers=hdr)
        r.raise_for_status()
        full = r.json()
        data_block = full.get("data", {}) or {}
        volumes = _normalize_volumes(data_block)
        return templates.TemplateResponse(
            "record.html",
            {
                "request": request,
                "record": full,
                "volumes": volumes,
            },
        )

# ───────────────────────────────────────────────────────────────────────────────
# KEYS page: dataspace -> type -> object (kept for convenience)
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/keys", response_class=HTMLResponse)
async def keys_page(request: Request):
    prefill_ds = []
    try:
        at = _access_token(request)
        rows = await osdu.list_dataspaces(at)
        prefill_ds = [{"path": x.get("path", ""), "uri": x.get("uri", "")} for x in (rows or []) if x.get("path")]
    except Exception as e:
        log.warning("keys_page list_dataspaces failed: %s", e)
        prefill_ds = []
    return templates.TemplateResponse(
        "keys.html",
        {"request": request, "prefill_ds": prefill_ds},
        media_type="text/html",
    )


@app.get("/keys/dataspaces.json")
async def keys_dataspaces(request: Request):
    at = _access_token(request)
    try:
        rows = await osdu.list_dataspaces(at)
    except Exception as e:
        log.warning("keys_dataspaces failed: %s", e)
        rows = []
    items = [{"path": x.get("path"), "uri": x.get("uri")} for x in rows if x.get("path")]
    return JSONResponse({"items": items})


@app.get("/keys/types.json")
async def keys_types(
    request: Request,
    ds: str = Query(..., description="Dataspace path"),
    source: str = Query("live", description="'live' (Rddms) or 'catalog' (curated)"),
):
    at = _access_token(request)
    items: List[Dict[str, Any]] = []
    if source == "live":
        enc = urllib.parse.quote(ds, safe="")
        try:
            rows = await osdu.list_types(at, enc)
        except Exception as e:
            log.warning("keys_types list_types failed: %s", e)
            rows = []
        for r in rows or []:
            name = r.get("name") if isinstance(r, dict) else r
            count = r.get("count") if isinstance(r, dict) else None
            if name:
                items.append({"name": name, "count": count})
    else:
        # curated fallback list
        items = [{"name": x} for x in [
            "resqml20.obj_PropertyKind",
            "resqml20.obj_StringTableLookup",
            "resqml20.obj_LocalDepth3dCrs",
            "resqml20.obj_Grid2dRepresentation",
            "resqml20.obj_HorizonInterpretation",
            "resqml20.obj_GeneticBoundaryFeature",
            "resqml20.obj_IjkGridRepresentation",
            "resqml20.obj_ContinuousProperty",
            "resqml20.obj_CategoricalProperty",
            "resqml20.obj_DiscreteProperty",
            "resqml20.obj_OrganizationFeature",
            "resqml20.obj_TectonicBoundaryFeature",
            "resqml20.obj_Activity",
            "resqml20.obj_ActivityTemplate",
            "eml20.obj_EpcExternalPartReference",
        ]]
    return JSONResponse({"items": items})


@app.get("/keys/objects.json")
async def keys_objects(
    request: Request,
    ds: str = Query(..., description="Dataspace path"),
    typ: str = Query(..., description="resqml20.obj_* type"),
):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    rows = await osdu.list_resources(at, enc, typ)
    out = []
    for r in rows or []:
        uid = r.get("Uuid") or r.get("UUID") or r.get("uuid")
        if not uid:
            uri = r.get("uri") or ""
            if "(" in uri and ")" in uri:
                uid = uri.split("(")[-1].rstrip(")")
            else:
                uid = uri
        title = (r.get("Citation") or {}).get("Title") or r.get("name") or uid
        ct = r.get("$type") or r.get("contentType") or ""
        out.append({"uuid": uid, "title": title, "uri": r.get("uri", ""), "contentType": ct})
    return JSONResponse({"items": out})


@app.get("/keys/object.json")
async def keys_object(
    request: Request,
    ds: str = Query(...),
    typ: str = Query(...),
    uuid: str = Query(...),
):
    at = _access_token(request)
    enc = urllib.parse.quote(ds, safe="")
    # Metadata (with referencedContent - reserved for future)
    obj = await osdu.get_resource(at, enc, typ, uuid, include_refs=True)

    # --- Normalize possible list shapes to a single dict for UI ---
    raw_obj = obj  # keep a copy for return (diagnostics)
    if isinstance(obj, list):
        # pick the first dict item if present; else empty dict
        obj = next((x for x in obj if isinstance(x, dict)), {}) if obj else {}
    if not isinstance(obj, dict):
        # If we still don't have a dict, downgrade gracefully
        return JSONResponse({"obj": obj, "raw": raw_obj, "edges": [], "arrays": [], "geom": None})

    # Edges
    edges = osdu.extract_refs(obj) if hasattr(osdu, "extract_refs") else []

    # Arrays
    try:
        arrays = await osdu.list_arrays(at, enc, typ, uuid)
    except Exception as e:
        log.warning("keys_object list_arrays failed: %s", e)
        arrays = []

    # Geometry (Grid2d only)
    t = (obj.get("$type") or obj.get("contentType") or "")
    is_grid2d = t.endswith("Grid2dRepresentation") or "obj_Grid2dRepresentation" in t
    geom = osdu.extract_grid2d_geometry(obj) if (is_grid2d and hasattr(osdu, "extract_grid2d_geometry")) else None

    # Normalize Volumes (if present)
    data_block = obj if isinstance(obj, dict) else {}
    volumes = _normalize_volumes(data_block)

    return JSONResponse({"obj": obj, "raw": raw_obj, "edges": edges, "arrays": arrays, "geom": geom, "volumes": volumes})

# ───────────────────────────────────────────────────────────────────────────────
# Dataspace admin endpoints (delete/lock/unlock/manifest)
# ───────────────────────────────────────────────────────────────────────────────
@app.post("/dataspaces/delete", summary="Delete a dataspace")
async def dataspaces_delete(request: Request, path: str = Form(...)):
    at = _access_token(request)
    try:
        await osdu.delete_dataspace(at, path)
    except HTTPStatusError as e:
        r = e.response
        return JSONResponse(
            {
                "status": "error",
                "code": r.status_code,
                "reason": r.reason_phrase,
                "detail": (r.text[:2000] if r.text else ""),
            },
            status_code=r.status_code or 500,
        )
    return JSONResponse({"status": "ok"})


@app.post("/dataspaces/lock", summary="Lock a dataspace")
async def dataspaces_lock(request: Request, path: str = Form(...)):
    at = _access_token(request)
    try:
        await osdu.lock_dataspace(at, path)
    except HTTPStatusError as e:
        r = e.response
        return JSONResponse(
            {"status": "error", "code": r.status_code, "reason": r.reason_phrase, "detail": (r.text[:2000] if r.text else "")},
            status_code=r.status_code or 500,
        )
    return JSONResponse({"status": "ok"})


@app.post("/dataspaces/unlock", summary="Unlock a dataspace")
async def dataspaces_unlock(request: Request, path: str = Form(...)):
    at = _access_token(request)
    try:
        await osdu.unlock_dataspace(at, path)
    except HTTPStatusError as e:
        r = e.response
        return JSONResponse(
            {"status": "error", "code": r.status_code, "reason": r.reason_phrase, "detail": (r.text[:2000] if r.text else "")},
            status_code=r.status_code or 500,
        )
    return JSONResponse({"status": "ok"})


@app.post("/dataspaces/manifest", summary="Build OSDU manifest for a dataspace")
async def dataspaces_manifest(
    request: Request,
    path: str = Form(...),
    legal: str = Form(osdu.DEFAULT_LEGAL_TAG),
    owners: str = Form(",".join(osdu.DEFAULT_OWNERS)),
    viewers: str = Form(",".join(osdu.DEFAULT_VIEWERS)),
    countries: str = Form(",".join(osdu.DEFAULT_COUNTRIES)),
    create_missing: bool = Form(True),
):
    at = _access_token(request)
    try:
        manifest = await osdu.build_manifest(
            at,
            path,
            legal_tag=legal,
            owners=[x.strip() for x in owners.split(",") if x.strip()],
            viewers=[x.strip() for x in viewers.split(",") if x.strip()],
            countries=[x.strip() for x in countries.split(",") if x.strip()],
            create_missing_refs=create_missing,
        )
    except HTTPStatusError as e:
        r = e.response
        return JSONResponse(
            {"status": "error", "code": r.status_code, "reason": r.reason_phrase, "detail": (r.text[:2000] if r.text else "")},
            status_code=r.status_code or 500,
        )
    return JSONResponse({"status": "ok", "manifest": manifest})

>>>>>>> abb6fabfaa4de9d23e2761584cfe98385c375ea4
