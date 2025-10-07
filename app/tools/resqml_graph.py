#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json, base64, argparse, collections
import httpx
import networkx as nx
from pyvis.network import Network
from dotenv import load_dotenv

def _token_url(tenant): return f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
def _rddms_base(host):  return f"https://{host}/api/reservoir-ddms/v2"

def _aad_token_from_refresh(rt:str, tenant:str, client_id:str, scope:str) -> str:
    data = {
        "grant_type":"refresh_token",
        "refresh_token":rt,
        "client_id":client_id,
        "scope":scope or "openid offline_access",
    }
    r = httpx.post(_token_url(tenant), data=data, headers={"Content-Type":"application/x-www-form-urlencoded"}, timeout=90)
    r.raise_for_status()
    js = r.json()
    tok = js.get("access_token")
    if not tok:
        raise RuntimeError(js.get("error_description") or js)
    return tok

def _hdr(tok:str, partition:str) -> dict:
    return {
        "Authorization": f"Bearer {tok}",
        "data-partition-id": partition or "data",
        "accept": "application/json",
        "content-type": "application/json",
    }

def _get_resource(host, tok, partition, ds_enc, typ, uuid) -> dict:
    url = f"{_rddms_base(host)}/dataspaces/{ds_enc}/resources/{typ}/{uuid}"
    params = {"$format":"json", "referencedContent":"true", "arrayMetadata":"false", "arrayValues":"false"}
    r = httpx.get(url, headers=_hdr(tok, partition), params=params, timeout=90)
    r.raise_for_status()
    js = r.json()
    return js[0] if isinstance(js, list) and js else js

def _extract_refs(obj:dict) -> tuple[list[dict], list[dict]]:
    """Return (targets, sources) as list of {uuid, contentType, title}."""
    targets, sources = [], []

    def add_ref(lst, dor):
        if not isinstance(dor, dict): return
        ct = dor.get("ContentType") or dor.get("contentType")
        uu = dor.get("UUID") or dor.get("Uuid") or dor.get("uuid")
        ti = dor.get("Title") or dor.get("title") or ""
        if ct and uu:
            lst.append({"uuid": uu, "contentType": ct, "title": ti})

    def walk(x):
        if isinstance(x, dict):
            # DataObjectReference?
            if "ContentType" in x and ("UUID" in x or "Uuid" in x or "uuid" in x):
                # heuristic: inside obj, treat DORs under keys like 'Represented...' as target,
                # and DORs that point back to this as sources (rarely explicit). We’ll classify all as targets.
                add_ref(targets, x);  # conservative
            for v in x.values(): walk(v)
        elif isinstance(x, list):
            for v in x: walk(v)

    walk(obj)
    # De-duplicate by uuid/contentType
    def dedup(lst):
        seen=set(); out=[]
        for e in lst:
            key=(e["uuid"], e["contentType"])
            if key not in seen:
                seen.add(key); out.append(e)
        return out
    return dedup(targets), dedup(sources)

def bfs(host, tok, partition, ds, typ, uuid, depth:int=2) -> tuple[nx.Graph, dict]:
    ds_enc = __import__("urllib.parse").quote(ds, safe="")
    G = nx.Graph()
    queue = collections.deque([(typ, uuid, 0)])
    seen = set([(typ, uuid)])
    nodes = {}

    while queue:
        cur_typ, cur_uuid, d = queue.popleft()
        try:
            obj = _get_resource(host, tok, partition, ds_enc, cur_typ, cur_uuid)
        except httpx.HTTPStatusError as e:
            # Keep graphing even if one fetch fails
            obj = {"$type": cur_typ, "Uuid": cur_uuid, "_error": str(e)}

        title = ""
        cit = obj.get("Citation") if isinstance(obj, dict) else {}
        if isinstance(cit, dict): title = cit.get("Title") or ""
        nodes[(cur_typ, cur_uuid)] = {"title": title}

        G.add_node(f"{cur_typ}:{cur_uuid}", label=title or cur_uuid, group=cur_typ)

        targets, sources = _extract_refs(obj)

        # Add edges to targets
        for t in targets:
            t_typ = t["contentType"].split("type=")[-1] if "type=" in t["contentType"] else t["contentType"]
            t_uuid = t["uuid"]
            G.add_node(f"{t_typ}:{t_uuid}", label=t.get("title") or t_uuid, group=t_typ)
            G.add_edge(f"{cur_typ}:{cur_uuid}", f"{t_typ}:{t_uuid}", kind="target")
            if d < depth and (t_typ, t_uuid) not in seen:
                seen.add((t_typ, t_uuid))
                queue.append((t_typ, t_uuid, d+1))

        # You could also walk 'sources' here if you assemble them explicitly in extract_refs.

    return G, nodes

def write_pyvis(G:nx.Graph, out_html:str):
    net = Network(height="800px", width="100%", bgcolor="#ffffff", font_color="#222")
    net.barnes_hut()  # stable layout
    for n, data in G.nodes(data=True):
        net.add_node(n, label=data.get("label") or n, title=n, group=data.get("group"))
    for u,v,data in G.edges(data=True):
        net.add_edge(u, v, title=data.get("kind",""))
    net.show(out_html)

def main():
    load_dotenv()
    ap = argparse.ArgumentParser(description="RESQML graph")
    ap.add_argument("--dataspace", required=True)
    ap.add_argument("--type", required=True, help="e.g. resqml20.obj_Grid2dRepresentation")
    ap.add_argument("--uuid", required=True)
    ap.add_argument("--depth", type=int, default=2)
    ap.add_argument("--out", default="graph.html")
    ap.add_argument("--graphml", default=None)
    args = ap.parse_args()

    tenant = os.getenv("AZURE_TENANT_ID", "")
    client_id = os.getenv("AZURE_CLIENT_ID", "")
    scope = os.getenv("AZURE_SCOPE", "openid offline_access")
    partition = os.getenv("DATA_PARTITION_ID", "data")
    host = os.getenv("OSDU_BASE_URL", "equinordev.energy.azure.com")
    rt = os.getenv("REFRESH_TOKEN") or os.getenv("refresh_token")

    if not rt or not tenant or not client_id:
        sys.exit("Missing REFRESH_TOKEN / AZURE_TENANT_ID / AZURE_CLIENT_ID in environment (.env).")

    tok = _aad_token_from_refresh(rt, tenant, client_id, scope)

    G, nodes = bfs(host, tok, partition, args.dataspace, args.type, args.uuid, depth=args.depth)

    if args.graphml:
        nx.write_graphml(G, args.graphml)
        print(f"GraphML written: {args.graphml}")

    write_pyvis(G, args.out)
    print(f"Interactive HTML written: {args.out}")

if __name__ == "__main__":
