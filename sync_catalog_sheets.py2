import os, sys, time, re, html, logging
from io import StringIO
import requests, pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError as ReqConnError

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("sync")

# --------- ENV (GitHub Secrets / Variables) ----------
WOO_BASE_URL       = os.environ["WOO_BASE_URL"].rstrip("/")
WOO_KEY            = os.environ["WOO_KEY"]
WOO_SECRET         = os.environ["WOO_SECRET"]
SHEET_ID           = os.environ["SHEET_ID"]
CREDS_FILE         = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

BATCH_SLEEP        = float(os.getenv("BATCH_SLEEP", "0.10"))
PREFETCH_PRODUCTS  = (os.getenv("PREFETCH_PRODUCTS", "true").strip().lower() == "true")
MAX_IMAGES_PER_PRODUCT = int(float(os.getenv("MAX_IMAGES_PER_PRODUCT", "5")))
LIMIT_PRODUCTS     = int(float(os.getenv("LIMIT_PRODUCTS", "0")))
RETRY_MAX          = int(float(os.getenv("RETRY_MAX", "5")))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "2.0"))

# --------- Google Sheets helpers ----------
def open_sheet(sheet_id: str):
  scopes = ["https://www.googleapis.com/auth/spreadsheets"]
  creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
  gc = gspread.authorize(creds)
  return gc.open_by_key(sheet_id)

def get_ws(ss, name):
  try: return ss.worksheet(name)
  except gspread.WorksheetNotFound: return ss.add_worksheet(title=name, rows=1, cols=1)

def read_table(ws):
  vals = ws.get_all_values()
  if not vals: return [], []
  return vals[0], vals[1:]

def write_table(ws, headers, rows):
  ws.clear()
  ws.update([headers] + (rows or []), value_input_option="RAW")

def append_row(ws, row):
  ws.append_row(row, value_input_option="RAW")

# --------- Utils ----------
def to_float(x):
  if x is None: return 0.0
  s = str(x).strip()
  if s == "" or s.lower() in {"nan","n/a","na","-","—"}: return 0.0
  if s.endswith("%"): 
    try: return to_float(s[:-1]) / 100.0
    except: return 0.0
  s = re.sub(r"[^0-9,.\-]", "", s)
  if s in {"","-","--"}: return 0.0
  last_dot, last_comma = s.rfind("."), s.rfind(",")
  if last_comma > last_dot:
    s = s.replace(".","").replace(",",".")
  else:
    s = s.replace(",","")
  try: return float(s)
  except: return 0.0

def to_int(x):
  s = re.sub(r"[^0-9\-]", "", str(x or "").strip())
  if s in {"","-","--"}: return 0
  try: return int(float(s))
  except: return 0

def clean_cat_name(s: str) -> str:
  txt = html.unescape(str(s or ""))
  txt = txt.replace("\u00A0", " ")
  txt = re.sub(r"\s+", " ", txt).strip()
  return txt

def norm_cat_name(s: str) -> str:
  return clean_cat_name(s).lower()

def clean_path_display(path: str) -> str:
  if not path: return ""
  parts = [p for p in (p.strip() for p in str(path).split(">")) if p]
  parts = [clean_cat_name(p) for p in parts]
  return " > ".join(parts)

# --------- Woo client (retries + optional prefetch) ----------
class Woo:
  def __init__(self, base, key, secret, sleep=BATCH_SLEEP, prefetch=True):
    self.base = base + "/wp-json/wc/v3"
    self.session = requests.Session()
    self.session.auth = (key, secret)
    self.sleep = sleep
    self.retry_max = RETRY_MAX
    self.retry_backoff = RETRY_BACKOFF_BASE
    self._sku_index = {}
    self._cat_cache = None     # {(norm_name, parent_id): obj}
    self._cat_path_cache = {}  # {normalized_path: leaf_id}
    if prefetch:
      try: self.build_sku_index()
      except Exception as e: log.warning("Prefetch SKUs failed: %s", e)

  def _request(self, method, path, params=None, json_body=None):
    url = self.base + path
    attempt = 0
    while True:
      try:
        if method == "GET":
          r = self.session.get(url, params=params or {}, timeout=90)
        elif method == "POST":
          r = self.session.post(url, params=params or {}, json=json_body or {}, timeout=120)
        else:
          r = self.session.put(url, params=params or {}, json=json_body or {}, timeout=120)
        time.sleep(self.sleep)
        if r.status_code in (429,500,502,503,504) and attempt < self.retry_max:
          wait = (self.retry_backoff ** attempt) * (self.sleep if self.sleep>0 else 0.5)
          log.warning("HTTP %s %s → %d. Retry %d/%d in %.2fs", method, path, r.status_code, attempt+1, self.retry_max, wait)
          time.sleep(wait); attempt += 1; continue
        return r
      except (ReadTimeout, ConnectTimeout, ReqConnError) as e:
        if attempt < self.retry_max:
          wait = (self.retry_backoff ** attempt) * (self.sleep if self.sleep>0 else 0.5)
          log.warning("%s on %s %s. Retry %d/%d in %.2fs: %s", e.__class__.__name__, method, path, attempt+1, self.retry_max, wait, e)
          time.sleep(wait); attempt += 1; continue
        raise

  def _get(self, path, params=None):  return self._request("GET", path, params=params)
  def _post(self, path, json_body=None, params=None): return self._request("POST", path, params=params, json_body=json_body)
  def _put(self, path, json_body=None, params=None):  return self._request("PUT", path, params=params, json_body=json_body)

  # Products
  def build_sku_index(self):
    log.info("Prefetching SKUs …")
    page, count = 1, 0
    while True:
      r = self._get("/products", {"per_page":100, "page":page})
      if r.status_code != 200: raise RuntimeError(f"GET products p{page}: {r.status_code} {r.text[:200]}")
      arr = r.json()
      if not arr: break
      for p in arr:
        sku = str(p.get("sku") or "").strip(); pid = p.get("id")
        if sku and pid: self._sku_index[sku] = pid; count += 1
      if len(arr) < 100: break
      page += 1
    log.info("SKU index size: %d", count)

  def sku_to_id(self, sku):
    sku = str(sku or "").strip()
    if not sku: return None
    if sku in self._sku_index: return self._sku_index[sku]
    r = self._get("/products", {"sku": sku, "per_page":1})
    if r.status_code != 200: raise RuntimeError(f"GET product {sku}: {r.status_code} {r.text[:200]}")
    arr = r.json()
    if arr:
      pid = arr[0]["id"]; self._sku_index[sku] = pid; return pid
    return None

  def create_product(self, body):
    r = self._post("/products", body)
    if r.status_code not in (200,201): raise RuntimeError(f"POST product: {r.status_code} {r.text[:300]}")
    p = r.json()
    sku = str(p.get("sku") or "").strip()
    if sku and p.get("id"): self._sku_index[sku] = p["id"]
    return p

  def update_product(self, pid, body):
    r = self._put(f"/products/{pid}", body)
    if r.status_code != 200: raise RuntimeError(f"PUT product {pid}: {r.status_code} {r.text[:300]}")
    return r.json()

  # Categories
  def _load_cats(self):
    if self._cat_cache is not None: return self._cat_cache
    page, allc = 1, []
    while True:
      r = self._get("/products/categories", {"per_page":100, "page":page})
      if r.status_code != 200: raise RuntimeError(f"GET categories: {r.status_code} {r.text[:300]}")
      batch = r.json(); allc += batch
      if len(batch) < 100: break
      page += 1
    cache = {}
    for c in allc:
      name_key = norm_cat_name(c.get("name") or ""); parent = c.get("parent") or 0
      cache[(name_key, parent)] = c
    self._cat_cache = cache
    return cache

  def _find_cat_id(self, name, parent):
    c = self._load_cats().get((norm_cat_name(name), parent or 0))
    return c["id"] if c else None

  def _create_cat(self, name, parent):
    body = {"name": clean_cat_name(name)}
    if parent: body["parent"] = parent
    r = self._post("/products/categories", body)
    if r.status_code not in (200,201): raise RuntimeError(f"POST category {name}: {r.status_code} {r.text[:300]}")
    c = r.json()
    self._load_cats()[(norm_cat_name(c.get("name") or ""), c.get("parent") or 0)] = c
    return c["id"]

  def ensure_cat_path(self, path):
    if not path: return None
    norm_key = " > ".join(norm_cat_name(p) for p in [x.strip() for x in path.split(">") if x.strip()])
    if norm_key in self._cat_path_cache: return self._cat_path_cache[norm_key]
    parent, leaf = 0, None
    for part in [x.strip() for x in path.split(">") if x.strip()]:
      cid = self._find_cat_id(part, parent)
      if cid: leaf = cid; parent = cid
      else:   cid = self._create_cat(part, parent if parent else None); leaf = cid; parent = cid
    self._cat_path_cache[norm_key] = leaf
    return leaf

# --------- Category from RAW → STAGING ----------
def rebuild_supplier_category_map(ss):
  """Return dict {sku -> 'Chapter > Category'} vanuit RAW_SUPPLIER (schoon)."""
  raw = get_ws(ss, "RAW_SUPPLIER")
  headers, rows = read_table(raw)
  if not headers or not rows: return {}
  h = {h:i for i,h in enumerate(headers)}
  chap_ix = h.get("ChapterName", -1)
  cat_ix  = h.get("CategoryName", -1)
  sku_ix  = h.get("ArticleCode", -1)
  out = {}
  for r in rows:
    sku = (r[sku_ix] if sku_ix>=0 and sku_ix<len(r) else "").strip()
    if not sku: continue
    chap = clean_cat_name((r[chap_ix] if chap_ix>=0 and chap_ix<len(r) else "").strip())
    cat  = clean_cat_name((r[cat_ix]  if cat_ix>=0  and cat_ix<len(r)  else "").strip())
    path = f"{chap} > {cat}" if chap and cat else (chap or cat or "")
    out[sku] = path
  return out

def copy_categories_to_staging(ss):
  """Schrijf per SKU de supplier category naar STAGING.category_woo (overschrijft bestaande waarde)."""
  stg = get_ws(ss, "STAGING")
  headers, rows = read_table(stg)
  if not headers: return
  h = {h:i for i,h in enumerate(headers)}
  req_cols = ["sku","category_woo"]
  for c in req_cols:
    if c not in h:
      raise RuntimeError(f"STAGING mist kolom '{c}'")

  sup_map = rebuild_supplier_category_map(ss)
  if not sup_map:
    log.warning("Geen supplier-category map opgebouwd (RAW_SUPPLIER leeg?)")
    return

  # update in-memory rows
  for i, r in enumerate(rows):
    sku = (r[h["sku"]] if h["sku"] < len(r) else "").strip()
    if not sku: continue
    supplier_path = sup_map.get(sku, "")
    r[h["category_woo"]] = supplier_path  # altijd overschrijven

  # schrijf terug
  write_table(stg, headers, rows)
  log.info("STAGING.category_woo is bijgewerkt vanuit RAW_SUPPLIER (per SKU).")

# --------- Woo payload ----------
def to_woo_body(row, woo: Woo, idx):
  # images
  images_raw = (row[idx["images"]] if "images" in idx and idx["images"]<len(row) else "")
  urls = [u for u in (images_raw.split(",") if images_raw else []) if u]
  if MAX_IMAGES_PER_PRODUCT > 0: urls = urls[:MAX_IMAGES_PER_PRODUCT]
  imgs = [{"src": u} for u in urls]

  price_val = to_float(row[idx["price"]]) if "price" in idx else 0.0
  stock_val = int(to_float(row[idx["stock"]])) if "stock" in idx else 0
  cost_val  = to_float(row[idx["cost_price"]]) if "cost_price" in idx else 0.0

  body = {
    "name": (row[idx["name"]] if "name" in idx else "").strip(),
    "sku":  (row[idx["sku"]]  if "sku"  in idx else "").strip(),
    "regular_price": f"{price_val:.2f}",
    "description": (row[idx["long_description"]] if "long_description" in idx else "") or "",
    "short_description": (row[idx["short_description"]] if "short_description" in idx else "") or "",
    "manage_stock": True,
    "stock_quantity": stock_val,
    "images": imgs,
    "status": (row[idx["status"]] if "status" in idx else "publish") or "publish"
  }

  # tax class
  if "tax_class" in idx:
    val = (row[idx["tax_class"]] if idx["tax_class"]<len(row) else "").strip()
    if val: body["tax_class"] = val

  # categories
  cat_path = (row[idx["category_woo"]] if "category_woo" in idx else "").strip()
  if cat_path:
    try:
      path_clean = clean_path_display(cat_path)
      cid = woo.ensure_cat_path(path_clean)
      if cid: body["categories"] = [{"id": cid}]
      else:   log.warning("Geen category-id voor pad: '%s'", path_clean)
    except Exception as e:
      log.warning("Categorie aanmaken/koppelen faalde: %s (path='%s')", e, cat_path)

  # meta
  meta=[]
  if "brand" in idx:
    b = (row[idx["brand"]] if idx["brand"]<len(row) else "").strip()
    if b: meta.append({"key":"_brand","value":b})
  if cost_val > 0: meta.append({"key":"_cost_price","value":cost_val})
  if "ean" in idx:
    e = (row[idx["ean"]] if idx["ean"]<len(row) else "").strip()
    if e: meta.append({"key":"_ean","value":e})
  if meta: body["meta_data"] = meta

  return body

# --------- Main ----------
def main():
  ss = open_sheet(SHEET_ID)
  # zorg voor benodigde tabs
  for tab in ["RAW_SUPPLIER","STAGING","PUSH_LOG","ERRORS"]:
    get_ws(ss, tab)

  # 1) Neem categorie over: RAW_SUPPLIER → STAGING.category_woo
  copy_categories_to_staging(ss)

  # 2) Lees STAGING en push van boven naar beneden
  stg = get_ws(ss, "STAGING")
  headers, rows = read_table(stg)
  if not headers or not rows:
    log.info("STAGING empty; nothing to push."); return
  idx = {h:i for i,h in enumerate(headers)}

  woo = Woo(WOO_BASE_URL, WOO_KEY, WOO_SECRET, sleep=BATCH_SLEEP, prefetch=PREFETCH_PRODUCTS)

  if LIMIT_PRODUCTS > 0:
    rows = rows[:LIMIT_PRODUCTS]

  created = updated = errors = 0
  total = len(rows)
  log.info("Pushing %d products (top to bottom)…", total)

  for i, row in enumerate(rows, 1):
    try:
      sku  = (row[idx["sku"]] if "sku" in idx else "").strip()
      name = (row[idx["name"]] if "name" in idx else "").strip()
      price_val = to_float(row[idx["price"]]) if "price" in idx else 0.0

      if not sku:
        append_row(get_ws(ss,"ERRORS"), [time.strftime("%Y-%m-%d %H:%M:%S"), "skip", "", "Skipped: empty SKU", ""])
        continue
      if not name or price_val <= 0:
        append_row(get_ws(ss,"ERRORS"), [time.strftime("%Y-%m-%d %H:%M:%S"), "skip", sku, f"Skipped: name empty or price <= 0 (price='{row[idx['price']] if 'price' in idx else ''}')", ""])
        continue

      pid = woo.sku_to_id(sku)
      body = to_woo_body(row, woo, idx)
      if pid:
        woo.update_product(pid, body); updated += 1
      else:
        woo.create_product(body); created += 1

      if i % 25 == 0 or i == total:
        append_row(get_ws(ss,"PUSH_LOG"), [time.strftime("%Y-%m-%d %H:%M:%S"), f"progress {i}/{total}", "", "", f"new:{created} upd:{updated} err:{errors}"])

    except Exception as e:
      errors += 1
      append_row(get_ws(ss,"ERRORS"), [time.strftime("%Y-%m-%d %H:%M:%S"), "push", (row[idx["sku"]] if "sku" in idx else ""), str(e)[:500], ""])

  append_row(get_ws(ss,"PUSH_LOG"), [time.strftime("%Y-%m-%d %H:%M:%S"), "done", "", "", f"new:{created} upd:{updated} err:{errors}"])
  log.info("DONE new=%d upd=%d err=%d", created, updated, errors)

if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    sys.exit(130)
