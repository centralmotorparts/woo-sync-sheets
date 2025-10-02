import os, sys, time, math, csv, re, logging
from io import StringIO
import requests, pandas as pd
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("sync")

# ---- Env vars (komen uit GitHub Secrets) ----
SUPPLIER_CSV_URL = os.environ["SUPPLIER_CSV_URL"]
WOO_BASE_URL     = os.environ["WOO_BASE_URL"].rstrip("/")
WOO_KEY          = os.environ["WOO_KEY"]
WOO_SECRET       = os.environ["WOO_SECRET"]
SHEET_ID         = os.environ["SHEET_ID"]
BATCH_SLEEP      = float(os.getenv("BATCH_SLEEP", "0.10"))
CREDS_FILE       = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

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

# Robuuste parsers (fix voor "could not convert string to float")
def to_float(x):
  """
  Accepteert o.a.: €1.234,56  |  1,234.56  |  12,34  |  35%  |  ' n/a '  |  '-'
  """
  if x is None: 
    return 0.0
  s = str(x).strip()
  if s == "" or s.lower() in {"nan", "n/a", "na", "-", "—"}:
    return 0.0

  # percentage?
  if s.endswith("%"):
    num = s[:-1].strip()
    v = to_float(num)
    return v / 100.0

  # verwijder valuta, spaties, niet-relevante tekens
  s = re.sub(r"[^0-9,.\-]", "", s)
  if s in {"", "-", "--"}:
    return 0.0

  # Detecteer EU (1.234,56) vs EN (1,234.56)
  last_dot = s.rfind(".")
  last_comma = s.rfind(",")
  if last_comma > last_dot:
    # EU: punten zijn duizendtallen, komma is decimaal
    s = s.replace(".", "")
    s = s.replace(",", ".")
  else:
    # EN: komma's zijn duizendtallen, punt is decimaal
    s = s.replace(",", "")

  try:
    return float(s)
  except ValueError:
    return 0.0

def to_int(x):
  """ Integers tolerant: pakt cijfers (en minteken), negeert overige tekens. """
  if x is None:
    return 0
  s = re.sub(r"[^0-9\-]", "", str(x).strip())
  if s in {"", "-", "--"}:
    return 0
  try:
    return int(float(s))
  except:
    return 0

def short_html(html, n=180):
  return re.sub(r"\s+", " ", str(html or "")).strip()[:n]

# ---- Woo client ----
class Woo:
  def __init__(self, base, key, secret, sleep=BATCH_SLEEP):
    self.base = base + "/wp-json/wc/v3"
    self.session = requests.Session()
    self.session.auth = (key, secret)
    self.sleep = sleep
    self._cat = None  # {(name_lower, parent_id): obj}

  def _get(self, path, params=None):
    r = self.session.get(self.base + path, params=params or {}, timeout=60)
    time.sleep(self.sleep); return r
  def _post(self, path, json_body=None):
    r = self.session.post(self.base + path, json=json_body or {}, timeout=60)
    time.sleep(self.sleep); return r
  def _put(self, path, json_body=None):
    r = self.session.put(self.base + path, json=json_body or {}, timeout=60)
    time.sleep(self.sleep); return r

  def product_by_sku(self, sku):
    r = self._get("/products", {"sku": sku, "per_page": 1})
    if r.status_code != 200: raise RuntimeError(f"GET product {sku}: {r.status_code} {r.text[:300]}")
    arr = r.json(); return arr[0] if arr else None

  def create_product(self, body):
    r = self._post("/products", body)
    if r.status_code not in (200,201): raise RuntimeError(f"POST product: {r.status_code} {r.text[:300]}")
    return r.json()

  def update_product(self, pid, body):
    r = self._put(f"/products/{pid}", body)
    if r.status_code != 200: raise RuntimeError(f"PUT product {pid}: {r.status_code} {r.text[:300]}")
    return r.json()

  # --- categories
  def _load_cats(self):
    if self._cat is not None: return self._cat
    page=1; allc=[]
    while True:
      r = self._get("/products/categories", {"per_page":100, "page":page})
      if r.status_code != 200: raise RuntimeError(f"GET categories: {r.status_code} {r.text[:300]}")
      batch = r.json(); allc += batch
      if len(batch) < 100: break
      page += 1
    cache={}
    for c in allc:
      name = (c.get("name") or "").strip().lower()
      parent = c.get("parent") or 0
      cache[(name, parent)] = c
    self._cat = cache
    return cache

  def _find_cat_id(self, name, parent):
    c = self._load_cats().get((name.strip().lower(), parent or 0))
    return c["id"] if c else None

  def _create_cat(self, name, parent):
    body = {"name": name.strip()}
    if parent: body["parent"] = parent
    r = self._post("/products/categories", body)
    if r.status_code not in (200,201): raise RuntimeError(f"POST category {name}: {r.status_code} {r.text[:300]}")
    c = r.json()
    self._load_cats()[(c["name"].strip().lower(), c.get("parent") or 0)] = c
    return c["id"]

  def ensure_cat_path(self, path):
    if not path: return None
    parts = [p.strip() for p in path.split(">") if p.strip()]
    parent = 0; leaf=None
    for name in parts:
      cid = self._find_cat_id(name, parent)
      if cid: leaf=cid; parent=cid
      else:
        cid = self._create_cat(name, parent if parent else None)
        leaf=cid; parent=cid
    return leaf

# ---- Load supplier CSV ----
def load_supplier_df(url):
  res = requests.get(url, timeout=120)
  if res.status_code != 200: raise RuntimeError(f"Supplier CSV {res.status_code}: {res.text[:300]}")
  text = res.text
  delim = detect_delimiter(text.splitlines()[0] if text else "")
  df = pd.read_csv(StringIO(text), sep=delim, dtype=str).fillna("")
  return df

def build_supplier_cat(row):
  chap = (row.get("ChapterName") or "").strip()
  cat  = (row.get("CategoryName") or "").strip()
  return f"{chap} > {cat}" if chap and cat else (chap or cat or "")

# ---- Read MAP & RULES from Sheet ----
def load_map_from_sheet(ss):
  ws = get_ws(ss, "MAP")
  headers, rows = read_table(ws)
  if not headers:
    write_table(ws, ["category_supplier","category_woo","tax_class","attribute_map_json"], [])
    return {}
  idx = {h:i for i,h in enumerate(headers)}
  m={}
  for r in rows:
    if not r: continue
    key = (r[idx.get("category_supplier", -1)] if idx.get("category_supplier", -1) >=0 else "").strip()
    if not key: continue
    m[key] = {
      "category_woo": (r[idx.get("category_woo", -1)] if idx.get("category_woo", -1)>=0 else "").strip(),
      "tax_class": (r[idx.get("tax_class", -1)] if idx.get("tax_class", -1)>=0 else "").strip(),
      "attribute_map_json": (r[idx.get("attribute_map_json", -1)] if idx.get("attribute_map_json", -1)>=0 else "").strip(),
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
    # margin_pct accepteert 0.35, 35 of 35%
    mp = to_float(r[idx["margin_pct"]])
    if mp > 1.0:
      mp = mp / 100.0
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

# ---- Transform RAW -> STAGING ----
def transform_to_staging(df_raw, mapping, rules):
  out=[]
  for _, row in df_raw.iterrows():
    sku = (row.get("ArticleCode") or "").strip()
    if not sku: continue
    long_desc = (row.get("HTMLDescriptionExtended") or "").strip() or \
                (row.get("DescriptionERP") or "").strip() or \
                (row.get("Description") or "").strip()
    short_desc = short_html(long_desc, 180)
    title = (row.get("ProductTitle") or row.get("Description") or sku).strip()
    supplier_cat = build_supplier_cat(row)
    mapped = mapping.get(supplier_cat, {"category_woo":"","tax_class":"","attribute_map_json":""})
    cost   = to_float(row.get("DealerPrice"))
    stock  = max(0, to_int(row.get("StockQuantity")))
    price  = apply_margin(cost, rules)
    imgs = [u for u in [(row.get("Picture1") or "").strip(),
                        (row.get("Picture2") or "").strip(),
                        (row.get("Picture3") or "").strip()] if u]
    out.append([
      sku, title, short_desc, long_desc, (row.get("BrandName") or "").strip(),
      mapped.get("category_woo",""), mapped.get("tax_class",""),
      cost, price, stock, "", ",".join(imgs), mapped.get("attribute_map_json","") or "{}", "publish"
    ])
  return out

def to_woo_body(row_dict, woo: Woo):
  imgs = [{"src": u} for u in (row_dict["images"].split(",") if row_dict["images"] else []) if u]
  body = {
    "name": row_dict["name"],
    "sku": str(row_dict["sku"]).strip(),
    "regular_price": f'{float(row_dict["price"]):.2f}',
    "description": row_dict.get("long_description") or "",
    "short_description": row_dict.get("short_description") or "",
    "manage_stock": True,
    "stock_quantity": int(float(row_dict.get("stock") or 0)),
    "images": imgs,
    "status": row_dict.get("status") or "publish"
  }
  if row_dict.get("tax_class"): body["tax_class"] = row_dict["tax_class"]
  cat_path = row_dict.get("category_woo") or ""
  if cat_path:
    try:
      cid = woo.ensure_cat_path(cat_path)
      if cid: body["categories"] = [{"id": cid}]
    except Exception as e:
      log.warning("Category failed for %s: %s", row_dict["sku"], e)
  meta=[]
  if row_dict.get("brand"): meta.append({"key":"_brand","value":row_dict["brand"]})
  if row_dict.get("cost_price"): meta.append({"key":"_cost_price","value":row_dict["cost_price"]})
  if row_dict.get("ean"): meta.append({"key":"_ean","value":row_dict["ean"]})
  if meta: body["meta_data"]=meta
  return body

def main():
  ss = open_sheet(SHEET_ID)

  # Ensure tabs exist
  headers_staging = ['sku','name','short_description','long_description','brand','category_woo','tax_class',
                     'cost_price','price','stock','ean','images','attributes_json','status']
  for tab in ["RAW_SUPPLIER","MAP","RULES","STAGING","PUSH_LOG","ERRORS"]:
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

  # Transform → STAGING
  staging_rows = transform_to_staging(df_raw, mapping, rules)
  write_table(get_ws(ss, "STAGING"), headers_staging, staging_rows)
  log.info("STAGING updated: %d rows", len(staging_rows))

  # Push to Woo
  woo = Woo(WOO_BASE_URL, WOO_KEY, WOO_SECRET, sleep=BATCH_SLEEP)
  h, rows = read_table(get_ws(ss, "STAGING"))
  idx = {h:i for i,h in enumerate(h)}
  created=updated=errors=0

  for i, r in enumerate(rows, 1):
    row = { name: r[idx[name]] if idx.get(name) is not None else "" for name in h }
    try:
      sku = str(row["sku"]).strip()
      if not sku: continue
      exists = woo.product_by_sku(sku)
      body = to_woo_body(row, woo)
      if exists:
        woo.update_product(exists["id"], body); updated += 1
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
