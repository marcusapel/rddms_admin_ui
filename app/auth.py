from __future__ import annotations
import os
import time
import base64
import hashlib
import secrets
from typing import Optional, Dict, Any
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from itsdangerous import URLSafeSerializer, BadSignature
from authlib.integrations.httpx_client import AsyncOAuth2Client

# -----------------------------------------------------------------------------
# Azure AD / Microsoft identity platform — Authorization Code + PKCE
# -----------------------------------------------------------------------------
TENANT = os.getenv("AZURE_TENANT_ID", "")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "")
SCOPES = os.getenv("AZURE_SCOPE", "openid offline_access").split()

AUTH_BASE = f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0"
AUTHZ_URL = f"{AUTH_BASE}/authorize"
TOKEN_URL = f"{AUTH_BASE}/token"
REDIRECT_URI = os.getenv("OIDC_REDIRECT_URI", "http://localhost:8000/auth/callback")

SECRET_KEY = os.getenv("SECRET_KEY", "change_me_32chars_min")
ser = URLSafeSerializer(SECRET_KEY, salt="rddms-admin-oidc")

router = APIRouter(tags=["auth"])


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _pkce_code() -> tuple[str, str]:
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).decode().rstrip("=")
    return verifier, challenge


async def _client() -> AsyncOAuth2Client:
    return AsyncOAuth2Client(
        client_id=CLIENT_ID,
        scope=SCOPES,
        redirect_uri=REDIRECT_URI,
    )


def get_access_token_from_cookie(request: Request) -> Optional[str]:
    raw = request.cookies.get("oidc_tokens")
    if not raw:
        return None
    try:
        js = ser.loads(raw)
        return js.get("access_token")
    except BadSignature:
        return None
    except Exception:
        return None


# Mint tokens from environment refresh_token (REFRESH_TOKEN or refresh_token)
async def tokens_from_env() -> Optional[Dict[str, Any]]:
    rt = os.getenv("REFRESH_TOKEN") or os.getenv("refresh_token")
    if not rt or not CLIENT_ID or not TENANT:
        return None

    async with await _client() as cli:
        # Do NOT pass client_id: AsyncOAuth2Client injects it automatically.
        token = await cli.fetch_token(
            TOKEN_URL,
            grant_type="refresh_token",
            refresh_token=rt,
            scope=" ".join(SCOPES),  # optional for AAD refresh; safe to include
        )

    return {
        "access_token": token.get("access_token"),
        "refresh_token": token.get("refresh_token") or rt,
        "expires_in": token.get("expires_in"),
        "id_token": token.get("id_token", ""),
    }


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@router.get("/login")
async def login(request: Request):
    if not TENANT or not CLIENT_ID:
        return RedirectResponse("/auth?e=cfg", status_code=302)

    code_verifier, code_challenge = _pkce_code()
    # We serialize a payload but also use the signed string itself as the 'state'
    state_payload = {
        "t": int(time.time()),
        "cv": code_verifier,
        "next": request.query_params.get("next") or "/",
    }
    state = ser.dumps(state_payload)

    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "response_mode": "query",
        "scope": " ".join(SCOPES),
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    url = f"{AUTHZ_URL}?{urlencode(params)}"
    resp = RedirectResponse(url, status_code=302)
    resp.set_cookie("oidc_state", state, httponly=True, samesite="lax")
    return resp


@router.get("/login/auto")
async def login_auto(request: Request):
    tokens = await tokens_from_env()
    if not tokens:
        return RedirectResponse("/auth?e=noenv", status_code=302)
    next_url = request.query_params.get("next") or "/"
    resp = RedirectResponse(next_url, status_code=302)
    resp.set_cookie("oidc_tokens", ser.dumps(tokens), httponly=True, samesite="lax")
    resp.delete_cookie("oidc_state")
    return resp


@router.get("/auth/callback")
async def callback(request: Request):
    code = request.query_params.get("code")
    state_param = request.query_params.get("state")
    state_cookie = request.cookies.get("oidc_state")

    if not code or not state_cookie:
        return RedirectResponse("/auth?e=missing", status_code=302)
    if not state_param or state_param != state_cookie:
        return RedirectResponse("/auth?e=state", status_code=302)

    try:
        st = ser.loads(state_cookie)
    except BadSignature:
        return RedirectResponse("/auth?e=state", status_code=302)

    code_verifier = st.get("cv")
    next_url = st.get("next") or "/"

    async with await _client() as cli:
        token = await cli.fetch_token(
            TOKEN_URL,
            grant_type="authorization_code",
            code=code,
            code_verifier=code_verifier,
            client_id=CLIENT_ID,
            redirect_uri=REDIRECT_URI,
        )

    tokens = {
        "access_token": token.get("access_token"),
        "refresh_token": token.get("refresh_token"),
        "expires_in": token.get("expires_in"),
        "id_token": token.get("id_token", ""),
    }
    resp = RedirectResponse(next_url, status_code=302)
    resp.set_cookie("oidc_tokens", ser.dumps(tokens), httponly=True, samesite="lax")
    resp.delete_cookie("oidc_state")
    return resp


@router.get("/logout")
async def logout():
    resp = RedirectResponse("/", status_code=302)
    resp.delete_cookie("oidc_tokens")
    return resp


@router.get("/auth")
async def auth_info():
    return {
        "azure_tenant": TENANT[:8] + "..." if TENANT else "",
        "client_id": CLIENT_ID[:8] + "..." if CLIENT_ID else "",
        "redirect_uri": REDIRECT_URI,
        "scopes": SCOPES,
    }