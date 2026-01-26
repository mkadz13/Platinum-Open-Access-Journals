#!/usr/bin/env python3
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


def load_doaj(doaj_csv: Path) -> pd.DataFrame:
    doaj = pd.read_csv(doaj_csv, low_memory=False)

    # Filter to platinum/diamond OA per your spec
    doaj = doaj[
        (doaj["Does the journal comply to DOAJ's definition of open access?"] == "Yes")
        & (doaj["APC"] == "No")
    ].copy()

    doaj["title_norm"] = doaj["Journal title"].map(norm_title)
    doaj["issn_print"] = doaj["Journal ISSN (print version)"].map(norm_issn)
    doaj["issn_e"] = doaj["Journal EISSN (online version)"].map(norm_issn)

    # Build an ISSN lookup table (one row per ISSN)
    issn_rows = []
    for _, r in doaj.iterrows():
        for issn in (r["issn_print"], r["issn_e"]):
            if issn:
                issn_rows.append(
                    {
                        "issn": issn,
                        "Journal title": r["Journal title"],
                        "Journal URL": r.get("Journal URL", ""),
                        "DOAJ URL": r.get("URL in DOAJ", ""),
                    }
                )
    doaj_issn = pd.DataFrame(issn_rows).drop_duplicates("issn")

    doaj_titles = doaj[["title_norm", "Journal title", "Journal URL", "URL in DOAJ"]].drop_duplicates(
        "title_norm"
    ).rename(columns={"URL in DOAJ": "DOAJ URL"})

    return doaj_issn, doaj_titles


def load_jif(jif_csv: Path) -> pd.DataFrame:
    jif = pd.read_csv(jif_csv)
    # Normalize expected columns across your two formats
    cols = {c.lower(): c for c in jif.columns}

    if "name" not in cols:
        raise ValueError(f"{jif_csv} missing a 'name' column (case-insensitive).")

    name_col = cols["name"]
    score_col = cols.get("score") or cols.get("impact factor") or cols.get("jif")
    if score_col is None:
        raise ValueError(f"{jif_csv} missing an impact factor column (expected 'score').")

    out = pd.DataFrame(
        {
            "Journal": jif[name_col].astype(str).str.strip(),
            "Impact Factor": jif[score_col],
        }
    )

    # Optional ISSN in 2020 file
    if "issn" in cols:
        out["issn_norm"] = jif[cols["issn"]].map(norm_issn)
    else:
        out["issn_norm"] = None

    out["title_norm"] = out["Journal"].map(norm_title)

    # Make Impact Factor numeric where possible; keep non-numeric as NaN
    out["Impact Factor"] = pd.to_numeric(out["Impact Factor"], errors="coerce")

    return out


def match(jif: pd.DataFrame, doaj_issn: pd.DataFrame, doaj_titles: pd.DataFrame) -> pd.DataFrame:
    # 1) ISSN match (best)
    m_issn = jif.dropna(subset=["issn_norm"]).merge(
        doaj_issn, left_on="issn_norm", right_on="issn", how="inner"
    )

    # 2) Title match for everything else (or if ISSN missing)
    m_title = jif.merge(doaj_titles, on="title_norm", how="inner")

    # Combine, prefer ISSN result if duplicated
    keep_cols = ["Journal", "Impact Factor", "Journal URL", "DOAJ URL"]
    a = m_issn.rename(columns={"name": "Journal"})  # harmless if not present
    a["Journal URL"] = a.get("Journal URL", "")
    a["DOAJ URL"] = a.get("DOAJ URL", "")
    a = a.rename(columns={"Journal": "Journal"})  # no-op

    b = m_title.rename(columns={"Journal": "Journal"})
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


def to_markdown(df: pd.DataFrame, year_label: str) -> str:
    lines = []
    lines.append(f"# Platinum (diamond) OA journals with Impact Factors — {year_label}")
    lines.append("")
    lines.append(
        "Criteria: DOAJ-compliant Open Access = Yes, APC = No; matched to the provided JIF list."
    )
    lines.append("")
    lines.append("")

    lines.append("| Journal | Impact Factor | Links |")
    lines.append("|---|---:|---|")

    for _, r in df.iterrows():
        j = str(r["Journal"])
        jif = r["Impact Factor"]
        jif_str = f"{jif:.3f}" if pd.notna(jif) else ""
        journal_url = str(r.get("Journal URL") or "").strip()
        doaj_url = str(r.get("DOAJ URL") or "").strip()

        links = []
        if journal_url:
            links.append(f"[Journal site]({journal_url})")
        if doaj_url:
            links.append(f"[DOAJ]({doaj_url})")

        lines.append(f"| {j} | {jif_str} | {' · '.join(links)} |")

    lines.append("")
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--doaj", required=True, help="Path to journalcsv2.csv")
    p.add_argument("--jif", required=True, help="Path to a JIF CSV (e.g., JIF2021.csv)")
    p.add_argument("--label", required=True, help="Label for the output page (e.g., 2021)")
    p.add_argument("--out", default="docs/index.md", help="Output markdown path")
    args = p.parse_args()

    doaj_issn, doaj_titles = load_doaj(Path(args.doaj))
    jif = load_jif(Path(args.jif))
    matched = match(jif, doaj_issn, doaj_titles)

    out_md = to_markdown(matched, args.label)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(out_md, encoding="utf-8")
    print(f"Wrote {out_path} with {len(matched)} journals")


if __name__ == "__main__":
    main()
