#!/usr/bin/env python3
import re
import glob
from io import StringIO
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


def find_col(cols, options):
    cols_l = {c.lower().strip(): c for c in cols}
    for opt in options:
        key = opt.lower().strip()
        if key in cols_l:
            return cols_l[key]
    return None


def clean_jcr_csv_text(raw: str) -> str:
    out_lines = []
    for line in raw.splitlines():
        # Fix: "97.18"%,
        line = re.sub(r'"([0-9]+(?:\.[0-9]+)?)"%\s*,\s*$', r'"\1%"', line)
        # Fix: "0"%,
        line = re.sub(r'"([0-9]+)"%\s*,\s*$', r'"\1%"', line)
        # Fix: ..."%" ,  (general)
        line = re.sub(r'("%)\s*,\s*$', r"\1", line)
        out_lines.append(line)
    return "\n".join(out_lines) + "\n"


def read_jcr_csv_robust(path: str) -> pd.DataFrame:
    raw = Path(path).read_text(encoding="utf-8", errors="replace")

    # Remove the leading metadata line if present (the line starting with "Journal Data Filtered By:")
    lines = raw.splitlines()
    if lines and lines[0].strip().startswith('"Journal Data Filtered By:'):
        raw = "\n".join(lines[1:]) + "\n"

    raw = clean_jcr_csv_text(raw)
    return pd.read_csv(StringIO(raw), low_memory=False)


def main():
    # Change this folder + year
    year = 2023
    files = glob.glob(f"data/raw_jcr_{year}/*.csv")
    if not files:
        raise SystemExit("No CSVs found. Put them in data/raw_jcr_YYYY/")

    frames = []
    bad = []

    for f in files:
        try:
            df = read_jcr_csv_robust(f)
        except Exception as e:
            bad.append((f, f"read failed: {e}"))
            continue

        # Match your JCR headers
        name_col = find_col(df.columns, ["Journal name", "journal name", "name", "journal", "journal title"])
        score_col = find_col(df.columns, [f"{year} JIF", f"{year} jif", "jif", "impact factor", "score"])
        issn_col = find_col(df.columns, ["ISSN", "issn"])
        eissn_col = find_col(df.columns, ["eISSN", "eissn"])

        if not name_col or not score_col:
            bad.append((f, "missing required columns (Journal name + YEAR JIF)"))
            continue

        jif_raw = df[score_col].astype(str).str.strip()
        # Convert special values
        jif_raw = jif_raw.replace({"N/A": "", "nan": "", "<0.1": "0.05"})
        score = pd.to_numeric(jif_raw, errors="coerce")

        out = pd.DataFrame(
            {
                "name": df[name_col].astype(str).str.strip(),
                "score": score,
            }
        )

        # Prefer eISSN, fallback to ISSN (fewer identifiers / better match)
        eissn = df[eissn_col].map(norm_issn) if eissn_col else None
        pissn = df[issn_col].map(norm_issn) if issn_col else None

        out["issn"] = None
        if eissn_col:
            out["issn"] = eissn
        if issn_col:
            if out["issn"] is None:
                out["issn"] = pissn
            else:
                out["issn"] = out["issn"].fillna(pissn)

        out["title_norm"] = out["name"].map(norm_title)
        frames.append(out)

    if not frames:
        print("No usable CSVs. Problem files:")
        for bf, err in bad:
            print(f" - {bf}: {err}")
        raise SystemExit(1)

    all_df = pd.concat(frames, ignore_index=True)

    # Deduplicate: prefer ISSN if present, else title_norm
    all_df["key"] = all_df["issn"].fillna(all_df["title_norm"])

    # Keep highest score per key
    all_df = all_df.sort_values(["key", "score"], ascending=[True, False])
    all_df = all_df.drop_duplicates("key", keep="first")

    # Output in your expected format for matcher script
    all_df = all_df[["name", "score", "issn"]].sort_values("score", ascending=False)

    out_path = f"data/JIF{year}.csv"
    Path("data").mkdir(parents=True, exist_ok=True)
    all_df.to_csv(out_path, index=False)

    print(f"Wrote {out_path} with {len(all_df)} journals")

    if bad:
        print("\nSkipped/problem files:")
        for bf, err in bad:
            print(f" - {bf}: {err}")


if __name__ == "__main__":
    main()
