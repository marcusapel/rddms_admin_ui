from __future__ import annotations
import os
import urllib.parse
import logging

import numpy as np
import httpx
from fastapi import FastAPI, Request, Form, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import osdu
from .auth import (
    router as auth_router,
    get_access_token_from_cookie,
    tokens_from_env,
    ser,
)

# -----------------------------------------------------------------------------
# App setup
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
)
log = logging.getLogger("rddms-admin")

app = FastAPI(title="RDDMS Admin")

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

@app.middleware("http")
async def no_transform_headers(request, call_next):
    resp: Response = await call_next(request)
    resp.headers.setdefault("Cache-Control", "no-store, no-transform")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    return resp

app.include_router(auth_router)

app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)

# -----------------------------------------------------------------------------
# Middleware: auto-login via env refresh_token for GET/HEAD requests
# -----------------------------------------------------------------------------
@app.middleware("http")
async def auto_login_middleware(request: Request, call_next):
    path = request.url.path or "/"
    try:
        if (
            request.method in ("GET", "HEAD")
            and not request.cookies.get("oidc_tokens")
            and not path.startswith("/static")
            and not path.startswith("/auth")
            and not path.startswith("/login")
            and not path.startswith("/logout")
        ):
            tokens = await tokens_from_env()
            if tokens:
                resp = RedirectResponse(str(request.url), status_code=302)
                resp.set_cookie("oidc_tokens", ser.dumps(tokens), httponly=True, samesite="lax")
                return resp
    except Exception as e:
        log.warning("Auto-login via env refresh_token skipped: %s", e)

    return await call_next(request)


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _access_token(request: Request) -> str:
    at = get_access_token_from_cookie(request)
    if not at:
        raise HTTPException(401, "Please sign in: /login or /login/auto")
    return at


# -----------------------------------------------------------------------------
# Pages & actions
# -----------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse, summary="Home: list dataspaces")
async def home(request: Request):
    try:
        at = _access_token(request)
        dataspaces = await osdu.list_dataspaces(at)
    except Exception:
        dataspaces = []

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "view": "home",
            "dataspaces": dataspaces,
            # Defaults for the "Create Dataspace" form (prefilled values, not placeholders)
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
):
    at = _access_token(request)
    await osdu.create_dataspace(
        at,
        path,
        legal_tag=legal,
        owners=[x.strip() for x in owners.split(",") if x.strip()],
        viewers=[x.strip() for x in viewers.split(",") if x.strip()],
        countries=[x.strip() for x in countries.split(",") if x.strip()],
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


# -----------------------------------------------------------------------------
# Guided Create Forms (server endpoints)
# -----------------------------------------------------------------------------
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
    res = await osdu.create_resource(at, enc, "resqml20.obj_PropertyKind", body)
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
    res = await osdu.create_resource(at, enc, "resqml20.obj_StringTableLookup", body)
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
    res = await osdu.create_resource(at, enc, "resqml20.obj_LocalDepth3dCrs", body)
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
        "Origin": {
            "$type": "resqml20.Point3d",
            "Coordinate1": origin_x,
            "Coordinate2": origin_y,
            "Coordinate3": 0.0,
        },
        "Offset": [
            {
                "Offset": {"Coordinate1": u_x, "Coordinate2": u_y, "Coordinate3": 0.0},
                "Spacing": {"$type": "eml20.LengthMeasure", "Value": u_spacing},
            },
            {
                "Offset": {"Coordinate1": v_x, "Coordinate2": v_y, "Coordinate3": 0.0},
                "Spacing": {"$type": "eml20.LengthMeasure", "Value": v_spacing},
            },
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

    res = await osdu.create_resource(at, enc, "resqml20.obj_Grid2dRepresentation", body)
    return JSONResponse({"status": "ok", "created": res})


# -----------------------------------------------------------------------------
# Search
# -----------------------------------------------------------------------------
@app.get("/search", response_class=HTMLResponse, summary="Search form (OSDU search v2)")
async def search_page(request: Request):
    return templates.TemplateResponse("search.html", {"request": request})


@app.post(
    "/search/run",
    response_class=HTMLResponse,
    summary="Run OSDU /api/search/v2/query",
)
async def search_run(
    request: Request, kind: str = Form("*:*:*:*"), query: str = Form("*"), limit: int = Form(20)
):
    at = _access_token(request)
    url = f"https://{osdu.OSDU_BASE_URL}/api/search/v2/query"
    hdr = osdu.headers(at)
    payload = {"kind": kind, "query": query, "limit": int(limit)}

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=hdr, json=payload)
        r.raise_for_status()
        res = r.json()

    return templates.TemplateResponse(
        "search.html",
        {"request": request, "results": res, "kind": kind, "q": query},
    )
# -----------------------------------------------------------------------------
# KEYS page: dataspace -> type -> object
# -----------------------------------------------------------------------------
from fastapi import Query

# -----------------------------------------------------------------------------
# KEYS: page shell (prefill dataspaces server-side for robustness)
# -----------------------------------------------------------------------------
@app.get("/keys", response_class=HTMLResponse)
async def keys_page(request: Request):
    prefill_ds = []
    try:
        at = _access_token(request)
        rows = await osdu.list_dataspaces(at)
        # Normalize minimal shape for the template/JS
        prefill_ds = [{"path": x.get("path", ""), "uri": x.get("uri", "")}
                      for x in (rows or []) if x.get("path")]
    except Exception:
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
    except Exception:
        rows = []
    # Return minimal shape for the UI
    items = [{"path": x.get("path"), "uri": x.get("uri")} for x in rows if x.get("path")]
    return JSONResponse({"items": items})


@app.get("/keys/types.json")
async def keys_types(
    request: Request,
    ds: str = Query(..., description="Dataspace path"),
    source: str = Query("live", description="'live' (Rddms) or 'catalog' (curated)"),
):
    at = _access_token(request)
    items = []
    if source == "live":
        enc = urllib.parse.quote(ds, safe="")
        try:
            rows = await osdu.list_types(at, enc)
        except Exception:
            rows = []
        # Normalize to {"name": "...", "count": N}
        for r in rows or []:
            name = r.get("name") if isinstance(r, dict) else r
            count = r.get("count") if isinstance(r, dict) else None
            if name:
                items.append({"name": name, "count": count})
    else:
        # Curated seed of commonly used RESQML 2.0.1 object types; extend as needed.
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
    # Metadata (with referencedContent)
    obj = await osdu.get_resource(at, enc, typ, uuid, include_refs=True)
    # Edges
    edges = osdu.extract_refs(obj)
    # Arrays
    arrays = await osdu.list_arrays(at, enc, typ, uuid)
    # Geometry (if Grid2d)
    geom = (
        osdu.extract_grid2d_geometry(obj)
        if (obj.get("$type", "") or "").endswith("Grid2dRepresentation")
        else None
    )
    return JSONResponse({"obj": obj, "edges": edges, "arrays": arrays, "geom": geom})
