
# app/ingest_router.py
from __future__ import annotations
import os
import uuid
import json
from typing import Any, Dict, Optional
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse

from app.osdu import submit_workflow_run  # <<— use client from osdu.py

router = APIRouter()
_MAX_ITEMS = 100
_MANIFESTS: Dict[str, Dict[str, Any]] = {}

def _find_access_token(request: Request) -> Optional[str]:
    """Try to retrieve the access_token from common places used in the app."""
    access_token = None
    try:
        session = getattr(request, 'session', None)
        if isinstance(session, dict):
            access_token = session.get('access_token') or session.get('token')
    except Exception:
        access_token = None

    if not access_token:
        try:
            access_token = getattr(request.state, 'access_token', None)
        except Exception:
            access_token = None

    if not access_token:
        auth = request.headers.get('Authorization')
        if auth and auth.lower().startswith('bearer '):
            access_token = auth.split(' ', 1)[1]

    if not access_token:
        access_token = request.cookies.get('access_token')
    return access_token

@router.post("/manifest/ingest")
async def ingest_manifest(
    request: Request,
    background_tasks: BackgroundTasks,
) -> JSONResponse:
    """Accepts a manifest (JSON) and immediately triggers Osdu_ingest workflow."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    manifest = body.get("manifest")
    if not isinstance(manifest, dict):
        raise HTTPException(status_code=400, detail="Body must include a 'manifest' object")

    # store last few manifests (debug)
    manifest_id = str(uuid.uuid4())
    _MANIFESTS[manifest_id] = manifest
    if len(_MANIFESTS) > _MAX_ITEMS:
        try:
            oldest_key = next(iter(_MANIFESTS.keys()))
            _MANIFESTS.pop(oldest_key, None)
        except StopIteration:
            pass

    access_token = _find_access_token(request)
    if not access_token:
        raise HTTPException(status_code=401, detail="access_token not found in session/headers/cookies")

    try:
        workflow_response = await submit_workflow_run(
            access_token=access_token,
            manifest=manifest,
            partition=body.get("partition"),
            app_key=body.get("appKey"),
            run_id=body.get("runId"),
        )
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=502, detail={"message": "Failed to call Workflow Service", "error": str(ex)})

    return JSONResponse(
        {
            "status": "submitted",
            "manifestId": manifest_id,
            "runId": body.get("runId"),
            "workflowResponse": workflow_response,
        }
    )

@router.get("/manifest/last")
async def get_last_manifest() -> JSONResponse:
    if not _MANIFESTS:
        raise HTTPException(status_code=404, detail="No manifests stored yet")
    last_key = list(_MANIFESTS.keys())[-1]
    return JSONResponse({"manifestId": last_key, "manifest": _MANIFESTS[last_key]})
