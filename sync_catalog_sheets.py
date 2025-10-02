import os, sys, time, math, csv, re, logging, html
from io import StringIO
import requests, pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError as ReqConnError

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("sync")

# ---- Env vars (uit GitHub Secrets / Variables) ----
SUPPLIER_CSV_URL   = os.environ["SUPPLIER_CSV_URL"]
WOO_BASE_URL       = os.environ["WOO_BASE_URL"].rstrip("/")
WOO_KEY            = os.environ["WOO_KEY"]
WOO_SECRET         = os.environ["WOO_SECRET"]
SHEET_ID           = os.environ["SHEET_ID"]
CREDS_FILE         = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

BATCH_SLEEP        = float(os.getenv("BATCH_SLEEP", "0.10"))
PREFETCH_PRODUCTS  = (os.getenv("PREFETCH_PRODUCTS", "true").strip().lower() == "true")
MAX_IMAGES_PER_PRODUCT = int(float(os.getenv("MAX_IMAGES_PER_PRODUCT", "99")))
LIMIT_PRODUCTS     = int(float(os.getenv("LIMIT_PRODUCTS", "0")))
RETRY_MAX          = int(float(os.getenv("RETRY_MAX", "5")))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "2.0"))

# ---- Google Sheets helpers ----
def open_sheet(sheet_id: str):
  scopes = ["https://www.googleapis.com/auth/spreadsheets"]
  creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
  gc = gspread.authorize(creds)
  return gc.open_by_key(sheet_id)

def get_ws(ss, name):
  try:
    return ss.worksheet(name)
  except gspread.WorksheetNotFound:
    return ss.add_worksheet(title=name, rows=1, cols=1)

def write_table(ws, headers, rows):
  ws.clear()
  ws.update([headers] + (rows or []), value_input_option="RAW")

def read_table(ws):
  vals = ws.get_all_values()
  if not vals: return [], []
  return vals[0], vals[1:]

def append_row(ws, row):
  ws.append_row(row, value_input_option="RAW")

# ---- Utils ----
def detect_delimiter(first_line):
  return ";" if first_line.count(";") > first_line.count(",") else ","

def to_float(x):
  """ Accepteert €1.234,56 / 1,234.56 / 12,34 / 35% / n/a / - """
  if x is None: 
    return 0.0
  s = str(x).strip()
  if s == "" or s.lower() in {"nan", "n/a", "na", "-", "—"}:
    return 0.0
  if s.endswith("%"):
    return to_float(s[:-1]) / 100.0
  s = re.sub(r"[^0-9,.\-]", "", s)
  if s in {"", "-", "--"}: return 0.0
  last_dot, last_comma = s.rfind("."), s.rfind(",")
  if last_comma > last_dot:
    s = s.replace(".", "").replace(",", ".")
  else:
    s = s.replace(",", "")
  try:
    return float(s)
  except ValueError:
    return 0.0

def to_int(x):
  s = re.sub(r"[^0-9\-]", "", str(x or "").strip())
  if s in {"", "-", "--"}: return 0
  try:
    return int(float(s))
  except:
    return 0

def short_html(html_txt, n=180):
  return re.sub(r"\s+", " ", str(html_txt or "")).strip()[:n]

# --- Categorie naam-normalisatie (fix voor &, &amp;, NBSP, dubbele spaties) ---
def clean_cat_name(s: str) -> str:
  """Display-naam: unescape, NBSP->spatie, spaties comprimeren, trim (behoud & en hoofdletters)"""
  txt = html.unescape(str(s or ""))
  txt = txt.replace("\u00A0", " ")
  txt = re.sub(r"\s+", " ", txt).strip()
  return txt

def norm_cat_name(s: str) -> str:
  """Lookup key: clean + lowercase"""
  return clean_cat_name(s).lower()

def norm_path_key(path: str) -> str:
  """Key voor volledige paden 'A > B > C' (genormaliseerd)"""
  if not path: return ""
  parts = [p for p in (p.strip() for p in str(path).split(">")) if p]
  parts = [norm_cat_name(p) for p in parts]
  return " > ".join(parts)

def clean_path_display(path: str) -> str:
  if not path: return ""
  parts = [p for p in (p.strip() for p in str(path).split(">")) if p]
  parts = [clean_cat_name(p) for p in parts]
  return " > ".join(parts)

# ---- Woo client (met retries & prefetch) ----
class Woo:
  def __init__(self, base, key, secret, sleep=BATCH_SLEEP, prefetch=True):
    self.base = base + "/wp-json/wc/v3"
    self.session = requests.Session()
    self.session.auth = (key, secret)
    self.sleep = sleep
    self.retry_max = RETRY_MAX
    self.retry_backoff = RETRY_BACKOFF_BASE

    self._cat = None               # {(norm_name, parent_id): obj}
    self._cat_path_cache = {}      # {norm_path_key: leaf_id}
    self._sku_index = {}           # {sku: id}

    if prefetch:
      try:
        self.build_sku_index()
      except Exception as e:
        log.warning("Prefetch SKUs failed (fallback to per-sku GET): %s", e)

  def _request(self, method, path, params=None, json_body=None):
    url = self.base + path
    attempt = 0
    while True:
      try:
        if method == "GET":
          r = self.session.get(url, params=params or {}, timeout=90)
        elif method == "POST":
          r = self.session.post(url, params=params or {}, json=json_body or {}, timeout=120)
        else:  # PUT
          r = self.session.put(url, params=params or {}, json=json_body or {}, timeout=120)

        # Soft throttling
        time.sleep(self.sleep)

        # Retry op 429/5xx
        if r.status_code in (429, 500, 502, 503, 504):
          if attempt < self.retry_max:
            wait = (self.retry_backoff ** attempt) * (self.sleep if self.sleep>0 else 0.5)
            log.warning("HTTP %s %s → %d. Retry %d/%d in %.2fs",
                        method, path, r.status_code, attempt+1, self.retry_max, wait)
            time.sleep(wait)
            attempt += 1
            continue
        return r

      except (ReadTimeout, ConnectTimeout, ReqConnError) as e:
        if attempt < self.retry_max:
          wait = (self.retry_backoff ** attempt) * (self.sleep if self.sleep>0 else 0.5)
          log.warning("%s on %s %s. Retry %d/%d in %.2fs: %s",
                      e.__class__.__name__, method, path, attempt+1, self.retry_max, wait, e)
          time.sleep(wait)
          attempt += 1
          continue
        raise

  def _get(self, path, params=None):
    return self._request("GET", path, params=params)
  def _post(self, path, json_body=None, params=None):
    return self._request("POST", path, params=params, json_body=json_body)
  def _put(self, path, json_body=None, params=None):
    return self._request("PUT", path, params=params, json_body=json_body)

  # ----- Products
  def build_sku_index(self):
    log.info("Prefetching all product SKUs …")
    page = 1
    count = 0
    while True:
      r = self._get("/products", {"per_page": 100, "page": page})
      if r.status_code != 200:
        raise RuntimeError(f"GET products page {page}: {r.status_code} {r.text[:200]}")
      arr = r.json()
      if not arr: break
      for p in arr:
        sku = str(p.get("sku") or "").strip()
        pid = p.get("id")
        if sku and pid:
          self._sku_index[sku] = pid
          count += 1
      if len(arr) < 100: break
      page += 1
    log.info("SKU index size: %d", count)

  def sku_to_id(self, sku: str):
    sku = str(sku or "").strip()
    if not sku: return None
    pid = self._sku_index.get(sku)
    if pid: return pid
    # fallback per-sku
    r = self._get("/products", {"sku": sku, "per_page": 1})
    if r.status_code != 200:
      raise RuntimeError(f"GET product {sku}: {r.status_code} {r.text[:200]}")
    arr = r.json()
    if arr:
      pid = arr[0]["id"]
      self._sku_index[sku] = pid
      return pid
    return None

  def create_product(self, body):
    r = self._post("/products", body)
    if r.status_code not in (200,201):
      raise RuntimeError(f"POST product: {r.status_code} {r.text[:300]}")
    p = r.json()
    sku = str(p.get("sku") or "").strip()
    if sku and p.get("id"):
      self._sku_index[sku] = p["id"]
    return p

  def update_product(self, pid, body):
    r = self._put(f"/products/{pid}", body)
    if r.status_code != 200:
      raise RuntimeError(f"PUT product {pid}: {r.status_code} {r.text[:300]}")
    return r.json()

  # ----- Categories
  def _load_cats(self):
    if self._cat is not None: return self._cat
    page=1; allc=[]
    while True:
      r = self._get("/products/categories", {"per_page":100, "page":page})
      if r.status_code != 200:
        raise RuntimeError(f"GET categories: {r.status_code} {r.text[:300]}")
      batch = r.json(); allc += batch
      if len(batch) < 100: break
      page += 1
    cache={}
    for c in allc:
      name_key = norm_cat_name(c.get("name") or "")
      parent = c.get("parent") or 0
      cache[(name_key, parent)] = c
    self._cat = cache
    return cache

  def _find_cat_id(self, name, parent):
    c = self._load_cats().get((norm_cat_name(name), parent or 0))
    return c["id"] if c else None

  def _create_cat(self, name, parent):
    clean_name = clean_cat_name(name)
    body = {"name": clean_name}
    if parent: body["parent"] = parent
    r = self._post("/products/categories", body)
    if r.status_code not in (200,201):
      raise RuntimeError(f"POST category {clean_name}: {r.status_code} {r.text[:300]}")
    c = r.json()
    self._load_cats()[(norm_cat_name(c.get("name") or ""), c.get("parent") or 0)] = c
    return c["id"]

  def ensure_cat_path(self, path):
    """Zorg dat 'A > B > C' bestaat; return leaf-id (met normalisatie en pad-cache)."""
    if not path: return None
    norm_key = norm_path_key(path)
    if norm_key in self._cat_path_cache:
      return self._cat_path_cache[norm_key]

    parts_raw = [p for p in (p.strip() for p in str(path).split(">")) if p]
    parent = 0; leaf=None
    for part in parts_raw:
      cid = self._find_cat_id(part, parent)
      if cid:
        leaf = cid; parent = cid
      else:
        cid = self._create_cat(part, parent if parent else None)
        leaf = cid; parent = cid

    self._cat_path_cache[norm_key] = leaf
    return leaf

# ---- Supplier CSV ----
def load_supplier_df(url):
  res = requests.get(url, timeout=120)
  if res.status_code != 200: raise RuntimeError(f"Supplier CSV {res.status_code}: {res.text[:300]}")
  text = res.text
  delim = detect_delimiter(text.splitlines()[0] if text else "")
  df = pd.read_csv(StringIO(text), sep=delim, dtype=str).fillna("")
  return df

def build_supplier_cat(row):
  chap = clean_cat_name((row.get("ChapterName") or "").strip())
  cat  = clean_cat_name((row.get("CategoryName") or "").strip())
  return f"{chap} > {cat}" if chap and cat else (chap or cat or "")

# ---- MAP & RULES from Sheet ----
def load_map_from_sheet(ss):
  ws = get_ws(ss, "MAP")
  headers, rows = read_table(ws)
  if not headers:
    write_table(ws, ["category_supplier","category_woo","tax_class","attribute_map_json"], [])
    return {}

  hdr_norm = [str(h or '').strip().lower() for h in headers]
  def col(name):
    try: return hdr_norm.index(name.lower())
    except ValueError: return -1

  ix_sup = col("category_supplier")
  ix_woo = col("category_woo")
  ix_tax = col("tax_class")
  ix_att = col("attribute_map_json")

  m={}
  for r in rows:
    if not r: continue
    supplier_raw = (r[ix_sup] if ix_sup >= 0 and ix_sup < len(r) else "").strip()
    if not supplier_raw: continue
    key_norm = norm_path_key(supplier_raw)
    m[key_norm] = {
      "category_woo": clean_path_display(r[ix_woo] if ix_woo >= 0 and ix_woo < len(r) else ""),
      "tax_class": (r[ix_tax] if ix_tax >= 0 and ix_tax < len(r) else "").strip(),
      "attribute_map_json": (r[ix_att] if ix_att >= 0 and ix_att < len(r) else "").strip(),
    }
  return m

def load_rules_from_sheet(ss):
  ws = get_ws(ss, "RULES")
  headers, rows = read_table(ws)
  if not headers:
    write_table(ws, ["min_cost","max_cost","margin_pct","round_to","price_end"], [
      [0,5,0.80,0.05,".95"],
      [5,20,0.50,0.05,".95"],
      [20,100,0.35,0.05,".95"],
      [100,999999,0.25,0.05,".95"],
    ])
    headers, rows = read_table(ws)
  idx = {h:i for i,h in enumerate(headers)}
  rules=[]
  for r in rows:
    if not any(r): continue
    mp = to_float(r[idx["margin_pct"]])
    if mp > 1.0: mp = mp / 100.0  # 35 / 35% -> 0.35
    rules.append({
      "min_cost": to_float(r[idx["min_cost"]]),
      "max_cost": to_float(r[idx["max_cost"]]),
      "margin_pct": mp,
      "round_to": to_float(r[idx["round_to"]]),
      "price_end": (r[idx["price_end"]] if idx.get("price_end") is not None else "").strip()
    })
  rules.sort(key=lambda x: x["min_cost"])
  return rules

def apply_margin(cost, rules):
  rule = None
  for rr in rules:
    if cost >= rr["min_cost"] and cost < rr["max_cost"]:
      rule = rr; break
  if rule is None: rule = rules[-1]
  price = cost * (1.0 + rule["margin_pct"])
  step = rule["round_to"]
  if step and step>0: price = round(price/step)*step
  end = rule["price_end"]
  if end:
    try:
      alt = float(f"{int(math.floor(price))}{end}")
      if alt > 0: price = alt
    except: pass
  return round(max(price, cost*1.05), 2)

# ---- Transform RAW -> STAGING (met mapping-fallback) ----
def transform_to_staging(df_raw, mapping, rules):
  out=[]
  missing_map_cnt = 0

  for _, row in df_raw.iterrows():
    sku = (row.get("ArticleCode") or "").strip()
    if not sku: 
      continue

    long_desc = (row.get("HTMLDescriptionExtended") or "").strip() or \
                (row.get("DescriptionERP") or "").strip() or \
                (row.get("Description") or "").strip()
    short_desc = short_html(long_desc, 180)
    title = (row.get("ProductTitle") or row.get("Description") or sku).strip()

    supplier_cat_display = build_supplier_cat(row)       # bv. "Zwembad > Pompen & Filters"
    supplier_key = norm_path_key(supplier_cat_display)   # genormaliseerde sleutel

    mapped = mapping.get(supplier_key, None)

    # FALLBACK: als MAP geen category_woo heeft, gebruik leverancierspad
    if mapped and mapped.get("category_woo"):
      category_woo_display = mapped["category_woo"]
      tax_class = mapped.get("tax_class") or ""
      attr_json = mapped.get("attribute_map_json") or "{}"
    else:
      category_woo_display = supplier_cat_display
      tax_class = ""
      attr_json = "{}"
      if not mapped:
        missing_map_cnt += 1

    cost   = to_float(row.get("DealerPrice"))
    stock  = max(0, to_int(row.get("StockQuantity")))
    price  = apply_margin(cost, rules)
    imgs = [u for u in [
      (row.get("Picture1") or "").strip(),
      (row.get("Picture2") or "").strip(),
      (row.get("Picture3") or "").strip()
    ] if u]

    out.append([
      sku, title, short_desc, long_desc, (row.get("BrandName") or "").strip(),
      category_woo_display,
      tax_class,
      cost, price, stock, "", ",".join(imgs), attr_json,
      "publish"
    ])

  if missing_map_cnt:
    log.info("MAP: %d supplier categorieën hadden geen mapping; fallback naar leverancier-pad gebruikt.", missing_map_cnt)

  return out

def to_woo_body(row_dict, woo: Woo):
  # images beperken (eerste run sneller)
  all_imgs = [u for u in (row_dict["images"].split(",") if row_dict["images"] else []) if u]
  if MAX_IMAGES_PER_PRODUCT > 0:
    all_imgs = all_imgs[:MAX_IMAGES_PER_PRODUCT]
  imgs = [{"src": u} for u in all_imgs]

  price_val = to_float(row_dict.get("price"))
  stock_val = int(to_float(row_dict.get("stock")))
  cost_val  = to_float(row_dict.get("cost_price"))

  body = {
    "name": row_dict["name"],
    "sku": str(row_dict["sku"]).strip(),
    "regular_price": f"{price_val:.2f}",
    "description": row_dict.get("long_description") or "",
    "short_description": row_dict.get("short_description") or "",
    "manage_stock": True,
    "stock_quantity": stock_val,
    "images": imgs,
    "status": row_dict.get("status") or "publish"
  }
  if row_dict.get("tax_class"): body["tax_class"] = row_dict["tax_class"]

  # categorie (pad) -> ID (normalisatie + padcache in Woo)
  cat_path_raw = row_dict.get("category_woo") or ""
  if cat_path_raw.strip():
    try:
      path_clean = clean_path_display(cat_path_raw)
      cid = woo.ensure_cat_path(path_clean)
      if cid:
        body["categories"] = [{"id": cid}]
      else:
        log.warning("No category id resolved for SKU %s (path='%s')", row_dict["sku"], path_clean)
    except Exception as e:
      log.warning("Category failed for %s: %s (path='%s')", row_dict["sku"], e, cat_path_raw)
  else:
    log.info("SKU %s has empty category_woo; product will use Woo default category.", row_dict["sku"])

  # meta
  meta=[]
  if row_dict.get("brand"):      meta.append({"key":"_brand","value":row_dict["brand"]})
  if cost_val > 0:               meta.append({"key":"_cost_price","value":cost_val})
  if row_dict.get("ean"):        meta.append({"key":"_ean","value":row_dict["ean"]})
  if meta: body["meta_data"] = meta
  return body

def write_diag_sheet(ss):
  """Schrijf DIAG tab met final category path en (indien aanwezig) MAP-pad voor snelle controle."""
  map_ws = get_ws(ss, "MAP")
  stg_ws = get_ws(ss, "STAGING")
  diag_ws = get_ws(ss, "DIAG")

  stg_headers, stg_rows = read_table(stg_ws)
  if not stg_headers or not stg_rows:
    write_table(diag_ws, ["notice"], [["STAGING is empty"]]); return
  h = {h:i for i,h in enumerate(stg_headers)}

  # Bouw mapping-index voor weergave
  map_headers, map_rows = read_table(map_ws)
  map_idx = {}
  if map_headers:
    mh = [str(x or '').strip().lower() for x in map_headers]
    def mcol(n): return mh.index(n) if n in mh else -1
    ix_sup = mcol("category_supplier")
    ix_woo = mcol("category_woo")
    for r in map_rows:
      sup = (r[ix_sup] if ix_sup>=0 and ix_sup<len(r) else "").strip()
      if not sup: continue
      map_idx[norm_path_key(sup)] = clean_path_display(r[ix_woo] if ix_woo>=0 and ix_woo<len(r) else "")

  rows_out = []
  limit = 300
  for i, r in enumerate(stg_rows):
    if i >= limit: break
    sku = r[h["sku"]]
    name = r[h["name"]]
    final_path = r[h["category_woo"]]
    rows_out.append([sku, name, final_path, map_idx.get(norm_path_key(final_path), "")])

  write_table(diag_ws, ["sku","name","final_category_path","mapped_path_from_MAP"], rows_out)

def main():
  ss = open_sheet(SHEET_ID)

  # Ensure tabs exist
  headers_staging = ['sku','name','short_description','long_description','brand','category_woo','tax_class',
                     'cost_price','price','stock','ean','images','attributes_json','status']
  for tab in ["RAW_SUPPLIER","MAP","RULES","STAGING","PUSH_LOG","ERRORS","DIAG"]:
    get_ws(ss, tab)

  # Load CSV → RAW_SUPPLIER
  log.info("Downloading supplier CSV …")
  res = requests.get(SUPPLIER_CSV_URL, timeout=120)
  if res.status_code != 200: raise RuntimeError(f"Supplier CSV {res.status_code}: {res.text[:300]}")
  text = res.text
  delim = detect_delimiter(text.splitlines()[0] if text else "")
  df_raw = pd.read_csv(StringIO(text), sep=delim, dtype=str).fillna("")
  write_table(get_ws(ss, "RAW_SUPPLIER"), list(df_raw.columns), df_raw.values.tolist())
  log.info("RAW_SUPPLIER updated: %d rows", len(df_raw))

  # MAP & RULES from Sheet
  mapping = load_map_from_sheet(ss)
  rules   = load_rules_from_sheet(ss)

  # Transform → STAGING (met fallback)
  staging_rows = transform_to_staging(df_raw, mapping, rules)
  write_table(get_ws(ss, "STAGING"), headers_staging, staging_rows)
  log.info("STAGING updated: %d rows", len(staging_rows))

  # Diagnose-overzicht
  write_diag_sheet(ss)
  log.info("DIAG written (first ~300 rows).")

  # Push naar Woo
  woo = Woo(WOO_BASE_URL, WOO_KEY, WOO_SECRET, sleep=BATCH_SLEEP, prefetch=PREFETCH_PRODUCTS)
  h, rows = read_table(get_ws(ss, "STAGING"))
  idx = {h:i for i,h in enumerate(h)}
  if LIMIT_PRODUCTS > 0:
    rows = rows[:LIMIT_PRODUCTS]

  created=updated=errors=0
  for i, r in enumerate(rows, 1):
    row = { name: r[idx[name]] if idx.get(name) is not None else "" for name in h }

    try:
      sku = str(row["sku"]).strip()
      if not sku: 
        append_row(get_ws(ss, "ERRORS"), [time.strftime("%Y-%m-%d %H:%M:%S"), "skip", "", "Skipped: empty SKU", ""])
        continue

      price_val = to_float(row.get("price"))
      name_val  = (row.get("name") or "").strip()
      if not name_val or price_val <= 0:
        append_row(get_ws(ss, "ERRORS"), [time.strftime("%Y-%m-%d %H:%M:%S"), "skip", sku, f"Skipped: name empty or price <= 0 (price='{row.get('price')}')", ""])
        continue

      pid = woo.sku_to_id(sku)
      body = to_woo_body(row, woo)
      if pid:
        woo.update_product(pid, body); updated += 1
      else:
        woo.create_product(body); created += 1

      if i % 25 == 0 or i == len(rows):
        append_row(get_ws(ss, "PUSH_LOG"), [time.strftime("%Y-%m-%d %H:%M:%S"), f"progress {i}/{len(rows)}", "", "", f"new:{created} upd:{updated} err:{errors}"])

    except Exception as e:
      errors += 1
      append_row(get_ws(ss, "ERRORS"), [time.strftime("%Y-%m-%d %H:%M:%S"), "push", row.get("sku",""), str(e)[:500], ""])

  append_row(get_ws(ss, "PUSH_LOG"), [time.strftime("%Y-%m-%d %H:%M:%S"), "done", "", "", f"new:{created} upd:{updated} err:{errors}"])
  log.info("DONE new=%d upd=%d err=%d", created, updated, errors)

if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    sys.exit(130)
