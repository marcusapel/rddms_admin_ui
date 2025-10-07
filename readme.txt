# INstall and run
pip install uvicorn fastapi httpx jinja2 numpy multipart
py -m uvicorn app.main:app --reload --port 8000 --env-file .\.env

# open http://localhost:8000

rddms-admin/
├─ requirements.txt
├─ .env
└─ app/
   ├─ main.py
   ├─ auth.py
   ├─ osdu.py
   ├─ templates/
   │  ├─ base.html
   │  ├─ index.html
   │  ├─ dataspace.html
   │  ├─ resource.html
   │  ├─ create.html
   │  ├─ _fragments.html
   |  └─ search.html
   └─ static/
      └─ app.js

sequenceDiagram
  participant U as User
  participant UI as Admin UI
  participant AAD as Microsoft Identity Platform
  participant RDDMS as Reservoir DDMS
  participant SEARCH as OSDU Search

  U->>UI: GET /login
  UI->>AAD: Redirect /authorize (PKCE)
  AAD->>UI: Redirect /auth/callback?code=...
  UI->>AAD: POST /token (code + code_verifier)
  AAD-->>UI: access_token (+refresh_token)
  U->>UI: Browse
  UI->>RDDMS: Bearer access_token (list types/arrays)
  UI->>SEARCH: POST /api/search/v2/query (data-partition-id)