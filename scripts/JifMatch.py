import re
import argparse
from pathlib import Path
import pandas as pd

def norm_title(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).lower().strip()
    s = s.replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_issn(x):
    if pd.isna(x):
        return None
    x = str(x).upper().strip()
    m = re.search(r"(\d{4})-?(\d{3}[0-9X])", x)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}"

def write_chunked_issns(issns, out_path: Path, chunk_size: int = 600) -> None:
    issns = [x for x in issns if x]
    issns = sorted(set(issns))

    lines = []
    total = len(issns)
    seg = 1
    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        lines.append(f"### SEGMENT {seg} ({start+1}-{end} of {total})")
        lines.append(", ".join(issns[start:end]))
        lines.append("") 
        seg += 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")

def _colmap(df: pd.DataFrame) -> dict:
    return {c.lower().strip(): c for c in df.columns}

def _find_col(df: pd.DataFrame, *candidates: str) -> str | None:
    cm = _colmap(df)
    for cand in candidates:
        key = cand.lower().strip()
        if key in cm:
            return cm[key]
    return None

def _norm_yesno(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().casefold()

def reduce_doaj(
    doaj_csv: Path,
    reduced_out_csv: Path,
    issn_out_txt: Path,
) -> pd.DataFrame:
    df = pd.read_csv(doaj_csv, low_memory=False)

    oa_col = _find_col(df, "Does the journal comply to DOAJ's definition of open access?")
    apc_col = _find_col(df, "APC")
    continues_col = _find_col(df, "Continues")
    continued_by_col = _find_col(df, "Continued By")

    if oa_col is None:
        raise KeyError("DOAJ CSV missing OA compliance column.")
    if apc_col is None:
        raise KeyError("DOAJ CSV missing APC column.")
    if continues_col is None:
        raise KeyError("DOAJ CSV missing 'Continues' column.")
    if continued_by_col is None:
        raise KeyError("DOAJ CSV missing 'Continued By' column.")

    df["_oa"] = df[oa_col].map(_norm_yesno)
    df["_apc"] = df[apc_col].map(_norm_yesno)

    def is_blank(x) -> bool:
        if pd.isna(x):
            return True
        return str(x).strip() == ""

    df["_continues_blank"] = df[continues_col].map(is_blank)
    df["_continued_by_blank"] = df[continued_by_col].map(is_blank)

    reduced = df[
        (df["_oa"] == "yes")
        & (df["_apc"] == "no")
        & (df["_continues_blank"])
        & (df["_continued_by_blank"])
    ].copy()

    reduced.drop(
        columns=["_oa", "_apc", "_continues_blank", "_continued_by_blank"],
        inplace=True,
    )

    keep = [
        "Journal title",
        "Journal ISSN (print version)",
        "Journal EISSN (online version)",
        "Journal URL",
        "URL in DOAJ",
        "Publisher",
        "Country of publisher",
        "APC",
        "APC amount",
        "Does the journal comply to DOAJ's definition of open access?",
        "Continues",
        "Continued By",
        "Subjects",
        "Added on Date",
        "Last updated Date",
        "Number of Article Records",
        "Most Recent Article Added",
    ]
    keep_existing = [c for c in keep if c in reduced.columns]
    reduced_out_csv.parent.mkdir(parents=True, exist_ok=True)
    reduced[keep_existing].to_csv(reduced_out_csv, index=False)

    issns = []

    for _, r in reduced.iterrows():
        e = norm_issn(r.get("Journal EISSN (online version)"))
        p = norm_issn(r.get("Journal ISSN (print version)"))
    
        if e:
            issns.append(e)
        elif p:
            issns.append(p)
    
    issns = sorted(set(issns))

    write_chunked_issns(issns, issn_out_txt, chunk_size=600)
    
    print(f"Reduced DOAJ saved: {reduced_out_csv} ({len(reduced)} journals)")
    print(f"ISSN list saved:   {issn_out_txt} ({len(issns)} ISSNs, chunked 600/segment)")


    return reduced


def load_doaj(reduced_doaj_csv: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    doaj = pd.read_csv(reduced_doaj_csv, low_memory=False)

    doaj["title_norm"] = doaj["Journal title"].map(norm_title)
    doaj["issn_print"] = doaj.get("Journal ISSN (print version)", pd.Series([None] * len(doaj))).map(norm_issn)
    doaj["issn_e"] = doaj.get("Journal EISSN (online version)", pd.Series([None] * len(doaj))).map(norm_issn)

    issn_rows = []
    for _, r in doaj.iterrows():
        for issn in (r.get("issn_print"), r.get("issn_e")):
            if issn:
                issn_rows.append(
                    {
                        "issn": issn,
                        "Journal title": r.get("Journal title", ""),
                        "Journal URL": r.get("Journal URL", ""),
                        "DOAJ URL": r.get("URL in DOAJ", ""),
                    }
                )
    doaj_issn = pd.DataFrame(issn_rows).drop_duplicates("issn")

    doaj_titles = (
        doaj[["title_norm", "Journal title", "Journal URL", "URL in DOAJ"]]
        .drop_duplicates("title_norm")
        .rename(columns={"URL in DOAJ": "DOAJ URL"})
    )

    return doaj_issn, doaj_titles


def load_jif(jif_csv: Path) -> pd.DataFrame:
    jif = pd.read_csv(jif_csv)
    cols = {c.lower(): c for c in jif.columns}

    if "name" not in cols:
        raise ValueError(f"{jif_csv} missing a 'name' column (case-insensitive).")

    name_col = cols["name"]
    score_col = cols.get("score") or cols.get("impact factor") or cols.get("jif")
    if score_col is None:
        raise ValueError(f"{jif_csv} missing an impact factor column (expected 'score' or 'impact factor' or 'jif').")

    out = pd.DataFrame(
        {
            "Journal": jif[name_col].astype(str).str.strip(),
            "Impact Factor": jif[score_col],
        }
    )

    if "issn" in cols:
        out["issn_norm"] = jif[cols["issn"]].map(norm_issn)
    else:
        out["issn_norm"] = None

    out["title_norm"] = out["Journal"].map(norm_title)
    out["Impact Factor"] = pd.to_numeric(out["Impact Factor"], errors="coerce")
    return out


def match(jif: pd.DataFrame, doaj_issn: pd.DataFrame, doaj_titles: pd.DataFrame) -> pd.DataFrame:
    m_issn = jif.dropna(subset=["issn_norm"]).merge(
        doaj_issn, left_on="issn_norm", right_on="issn", how="inner"
    )

    m_title = jif.merge(doaj_titles, on="title_norm", how="inner")
    keep_cols = ["Journal", "Impact Factor", "Journal URL", "DOAJ URL"]

    a = m_issn.copy()
    a["Journal URL"] = a.get("Journal URL", "")
    a["DOAJ URL"] = a.get("DOAJ URL", "")

    b = m_title.copy()
    b["Journal URL"] = b.get("Journal URL", "")
    b["DOAJ URL"] = b.get("DOAJ URL", "")

    combined = pd.concat(
        [
            a[keep_cols].copy(),
            b[keep_cols].copy(),
        ],
        ignore_index=True,
    )

    combined = combined.drop_duplicates(subset=["Journal"])
    combined = combined.sort_values(["Impact Factor", "Journal"], ascending=[False, True])
    combined = combined.reset_index(drop=True)
    return combined


def to_html(df: pd.DataFrame, year_label: str) -> str:
    df = df.copy()

    def link_cell(row):
        links = []
        if row.get("Journal URL"):
            links.append(f'<a href="{row["Journal URL"]}">Journal site</a>')
        if row.get("DOAJ URL"):
            links.append(f'<a href="{row["DOAJ URL"]}">DOAJ</a>')
        return " · ".join(links)

    df["Links"] = df.apply(link_cell, axis=1)
    df = df[["Journal", "Impact Factor", "Links"]]

    df["Impact Factor"] = df["Impact Factor"].map(lambda x: f"{x:.3f}" if pd.notna(x) else "")

    table_html = df.to_html(index=False, escape=False)
    table_html = table_html.replace(
        '<table border="1" class="dataframe">',
        '<table id="jifTable" border="1" class="dataframe">'
    )

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Platinum OA Journals - {year_label}</title>
  <style>
    body {{ font-family: system-ui, Arial, sans-serif; margin: 2rem; }}
    h1 {{ margin-bottom: 0.5rem; }}
    .meta {{ color: #555; margin-bottom: 1rem; }}

    .controls {{
      display: flex;
      gap: 0.5rem;
      flex-wrap: wrap;
      margin: 1rem 0;
      align-items: center;
    }}
    .controls input {{
      padding: 8px;
      border: 1px solid #ccc;
      border-radius: 8px;
    }}
    .controls button {{
      padding: 8px 12px;
      border: 1px solid #ccc;
      border-radius: 8px;
      background: #f5f5f5;
      cursor: pointer;
    }}
    .controls .hint {{
      width: 100%;
      color: #666;
      font-size: 0.9rem;
    }}

    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
    th {{ background: #f5f5f5; text-align: left; }}
    td:nth-child(2), th:nth-child(2) {{ text-align: right; white-space: nowrap; }}
  </style>
</head>
<body>
  <h1>Platinum OA journals with Impact Factors - {year_label}</h1>
  <div class="meta">Criteria: DOAJ OA-compliant = Yes, APC = No.</div>

  <div class="controls">
    <input id="q" type="text" placeholder="Search journal name..." />
    <input id="minIF" type="number" step="0.001" placeholder="Min IF" />
    <input id="maxIF" type="number" step="0.001" placeholder="Max IF" />
    <button id="clearBtn" type="button">Clear</button>
  </div>

  {table_html}

  <script>
  (function () {{
    const q = document.getElementById("q");
    const minIF = document.getElementById("minIF");
    const maxIF = document.getElementById("maxIF");
    const clearBtn = document.getElementById("clearBtn");
    const table = document.getElementById("jifTable");
    const tbody = table.tBodies[0];

    const rows = Array.from(tbody.rows);
    const cached = rows.map(row => {{
      const name = (row.cells[0].innerText || "").toLowerCase();
      const ifText = (row.cells[1].innerText || "").replace(/,/g, "").trim();
      let ifVal = Number(ifText);
      if (!Number.isFinite(ifVal)) {{
        if (ifText.startsWith("<")) {{
          const t = Number(ifText.slice(1).trim());
          ifVal = Number.isFinite(t) ? t : NaN;
        }} else {{
          ifVal = NaN;
        }}
      }}
      return {{ row, name, ifVal }};
    }});

    function applyFiltersNow() {{
      const query = (q.value || "").toLowerCase().trim();
      const min = minIF.value === "" ? -Infinity : Number(minIF.value);
      const max = maxIF.value === "" ? Infinity : Number(maxIF.value);

      for (const item of cached) {{
        const nameOK = query === "" || item.name.includes(query);
        const ifOK = Number.isFinite(item.ifVal) && item.ifVal >= min && item.ifVal <= max;
        item.row.style.display = (nameOK && ifOK) ? "" : "none";
      }}
    }}

    let t = null;
    function scheduleApply() {{
      if (t) clearTimeout(t);
      t = setTimeout(applyFiltersNow, 180); // tweak 150–250ms
    }}

    q.addEventListener("input", scheduleApply);
    minIF.addEventListener("input", scheduleApply);
    maxIF.addEventListener("input", scheduleApply);

    clearBtn.addEventListener("click", function () {{
      q.value = "";
      minIF.value = "";
      maxIF.value = "";
      applyFiltersNow();
    }});

    applyFiltersNow();
  }})();
  </script>
</body>
</html>
"""




def main():
    p = argparse.ArgumentParser()
    p.add_argument("--doaj", required=True, help="Path to DOAJ CSV (downloaded from https://doaj.org/csv)")
    p.add_argument("--jif", required=True, help="Path to a JIF CSV (e.g., JIF2024.csv)")
    p.add_argument("--label", required=True, help="Label for the output page (e.g., 2024)")
    p.add_argument("--out", default="docs/index.html", help="Output HTML path")

    p.add_argument("--reduced-doaj-out", default="data/doaj_diamond_active.csv",
                   help="Where to write the reduced DOAJ CSV")
    p.add_argument("--issn-out", default="data/doaj_issns.txt",
                   help="Where to write the ISSN list (one per line)")
    args = p.parse_args()

    reduced_path = Path(args.reduced_doaj_out)
    reduce_doaj(Path(args.doaj), reduced_path, Path(args.issn_out))

    doaj_issn, doaj_titles = load_doaj(reduced_path)
    jif = load_jif(Path(args.jif))
    matched = match(jif, doaj_issn, doaj_titles)

    out_html = to_html(matched, args.label)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(out_html, encoding="utf-8")
    print(f"Wrote {out_path} with {len(matched)} journals")


if __name__ == "__main__":
    main()
