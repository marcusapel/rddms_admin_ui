# OSDU RDDMS admin ui web client


## Install and run

Auth: 
store your adme refresh_token as env variable or in .env file

Requires: 
python libs: uvicorn fastapi httpx jinja2 numpy multipart

Call:
py -m uvicorn app.main:app --reload --port 8000 --env-file .\\.env

open http://127.0.0.1:8000/


### Design 

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
   │  ├─ keys.html
   │  ├─ resource.html
   │  ├─ create.html
   │  ├─ _fragments.html
   |  └─ search.html
   └─ static/
      └─ app.js

### Sequence Diagram

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
