#!/usr/bin/env python3
import re
import glob
import pandas as pd

def norm_title(s: str) -> str:
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
    cols_l = {c.lower(): c for c in cols}
    for opt in options:
        if opt.lower() in cols_l:
            return cols_l[opt.lower()]
    return None

def main():
    # Change this folder + year
    year = 2023
    files = glob.glob(f"data/raw_jcr_{year}/*.csv")
    if not files:
        raise SystemExit("No CSVs found. Put them in data/raw_jcr_YYYY/")

    frames = []
    for f in files:
        df = pd.read_csv(f, low_memory=False)
        name_col = find_col(df.columns, ["name", "journal", "journal name", "journal title"])
        score_col = find_col(df.columns, ["score", "impact factor", "jif", "journal impact factor"])
        issn_col  = find_col(df.columns, ["issn", "eissn", "issn (print)", "issn (online)"])

        if not name_col or not score_col:
            raise SystemExit(f"{f}: missing required columns (name + score)")

        out = pd.DataFrame({
            "name": df[name_col].astype(str).str.strip(),
            "score": pd.to_numeric(df[score_col], errors="coerce"),
        })

        if issn_col:
            out["issn"] = df[issn_col].map(norm_issn)
        else:
            out["issn"] = None

        out["title_norm"] = out["name"].map(norm_title)
        frames.append(out)

    all_df = pd.concat(frames, ignore_index=True)

    # Deduplicate: prefer ISSN if present, else title_norm
    all_df["key"] = all_df["issn"].fillna(all_df["title_norm"])

    # Keep the row with the highest score per key (or first if NaN)
    all_df = all_df.sort_values(["key", "score"], ascending=[True, False])
    all_df = all_df.drop_duplicates("key", keep="first")

    # Output in your expected format for the matcher script
    all_df = all_df[["name", "score", "issn"]].sort_values("score", ascending=False)

    out_path = f"data/JIF{year}.csv"
    all_df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(all_df)} journals")

if __name__ == "__main__":
    main()
