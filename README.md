FORGE-VA Wrapper

A standalone, runnable version of the FORGE-VA gaming-detection pipeline (Sharma, Fulton, Tomic, & Fulton — Identifying Structural Vulnerabilities in Healthcare Performance Measurement). Feed in SAIL and VAC3 facility performance data; get back ranked, facility-level gaming-risk scores.

All files are designed to sit in one flat directory — no packages, no subfolder imports beyond examples/.

What this is

FORGE-VA scores VA facilities for statistically implausible performance patterns consistent with gaming (see the manuscript for the full methodology). It combines three signal blocks:

Block 1 (SAIL Outcome Signal) — direction-adjusted, peer-relative standardized scores on gaming-susceptible access/scheduling measures.
Block 2 (VAC3 Behavioral Signal) — residual dynamics, reference-point proximity (formerly "bunching"), coordinated improvement, an exact global-covariance Mahalanobis distance, and an Isolation Forest fit per canonical measure group.
Block 3 (Coupling) — a rank-product term that rewards facilities flagged by both Block 1 and Block 2 simultaneously.

An ensemble score (forge_va) combines all three, weighted by each signal's empirical rank-biserial correlation against OIG-confirmed gaming cases.

This is semi-supervised, not unsupervised. Feature construction doesn't use labels; feature and block weighting does. That's why the tool has two modes — one that needs labels, one that doesn't.

Files in this directory
File	What it does
forge_va_cli.py	Entry point. Run this. Has fit and score subcommands.
forge_va_core.py	Shared math: rank normalization, rank-biserial effect size, AUC, and the CSV/Parquet auto-detecting file loader.
forge_va_block1.py	Builds Block 1 (SAIL) facility-level features.
forge_va_block2.py	Builds Block 2 (VAC3) facility-level features, including the exact Mahalanobis distance and per-measure Isolation Forest.
forge_va_fit.py	Combines blocks into scores, fits weights against ground truth, saves/loads/applies weights.
forge_va_matching.py	Fuzzy-matches OIG ground-truth facility names to your SAIL/VAC3 panel (handles typos, abbreviations, stopwords like "VA Medical Center").
examples/SAIL_example.csv	One real row + headers, showing exactly what a SAIL file needs to look like.
examples/VAC3_example.csv	Same, for VAC3.

Everything else you see (weights.json, scores.csv, scores_effect_sizes.csv, scores_top20.csv, the .parquet files, ground_truth_final.csv) is output or input data, not code — see below.

Setup
bash
pip install pandas numpy scipy scikit-learn rapidfuzz pyarrow

(pyarrow is only needed if you use Parquet inputs — see below. Everything else is required.)

Two modes
fit — run once, against labeled historical data

Learns which signals actually discriminate confirmed gaming cases from background facilities, and how much weight each one gets. Needs SAIL + VAC3 + a ground-truth file of OIG-confirmed cases.

bash
python forge_va_cli.py fit \
    --sail SAIL_FY15-FY24_Master.csv \
    --vac3 VAC3_all_years_combined_canon.csv \
    --ground-truth ground_truth_final.csv \
    --weights-out weights.json \
    --scores-out scores.csv \
    --top-k 20

This is the slow one — fitting an Isolation Forest per measure group across the full panel typically takes a minute or so.

Outputs:

weights.json — the fitted weights. Keep this; you need it for score.
scores.csv — every facility, every sub-feature, block scores, forge_va, forge_rank, classification, and oig_label (S/N/bg/unmatched).
scores_effect_sizes.csv — the full rank-biserial effect-size table: which signals were retained, which were excluded (r ≤ 0), and why.
scores_top20.csv (only if --top-k given) — just the top-K ranked facilities, printed to console and saved separately.
score — run any time, on new/unlabeled data

Applies weights you already fit to a new quarter's data. No ground truth needed — this is the "a new extract came in, rank it" path.

bash
python forge_va_cli.py score \
    --sail SAIL_FY26_Q1.csv \
    --vac3 VAC3_FY26_Q1.csv \
    --weights-in weights.json \
    --scores-out scores_new.csv \
    --top-k 20

Same output shape as fit, minus the ground-truth-dependent columns (no oig_label, no effect-size table — you're not re-fitting anything).

Input file formats

See examples/SAIL_example.csv and examples/VAC3_example.csv for a real, working one-row template of each — headers must match exactly.

SAIL needs: Facility, Measure, Score, Direction, FY, Quarter, VISN (plus whatever else your extract carries — extra columns are ignored).

VAC3 needs: Facility, Measure, Score, Direction, FY, Quarter, National_Benchmark, Regional_Benchmark.

Ground truth (for fit only) needs: sail_key, label — where label is S (confirmed), N (investigated, not substantiated), or A (ambiguous)/blank (never investigated).

One row per facility × measure × quarter observation. Direction should be something like "higher"/"lower" (or "higher is better"/"lower is better" — several phrasings are recognized).

CSV or Parquet — either works

Point --sail/--vac3 at a .csv or a .parquet file; the loader auto-detects by extension. Parquet is strongly recommended for GitHub: it compresses this kind of panel data 40–65x smaller than CSV (a 45MB CSV becomes roughly 1MB), which matters since GitHub warns at 50MB and blocks uploads outright past 100MB. Convert once:

python
import pandas as pd
df = pd.read_csv("SAIL_FY15-FY24_Master.csv", dtype={"FY": str, "Quarter": str, "VISN": str})
df["Score"] = pd.to_numeric(df["Score"], errors="coerce")  # required for Parquet
df.to_parquet("SAIL_FY15-FY24_Master.parquet")

Then just use the .parquet path in place of the .csv path — nothing else changes. Both formats produce byte-identical results.

Output columns worth knowing
forge_va — the ensemble risk score. Higher = more anomalous. Not a probability; it's a relative ranking signal.
forge_rank — integer rank, 1 = highest risk.
classification — high_priority (top --top-pct, default 10%), routine, or insufficient_data (missing Block 1 or Block 2 data — these facilities are not scored as low-risk, they're unscoreable; don't treat insufficient_data as a clean bill of health).
score_b1 / score_b2 / score_b3 — the three block scores, for diagnosing why a facility ranked where it did (e.g., outcome-dominant vs. behavior-dominant vs. coupled anomaly — see the manuscript's facility-decomposition discussion).
Known limitations
Ground-truth facility matching is an exact port of the notebook's fuzzy matcher, but always review the printed match audit (and any LOW-CONFIDENCE / UNMATCHED warnings) before trusting a fit run — a bad match silently changes which facilities anchor the weighting.
classification is a simple rank cutoff, not a calibrated probability of gaming. Treat it as a triage tool for prioritizing audit attention, not a verdict.
Facility counts from this wrapper may not land on exactly the manuscript's published Table 3 numbers even with identical formulas, since build_weighted_score_dual computes an ensemble score from whichever of Block 1/2/3 are present rather than requiring all three — see forge_va_fit.py's module docstring for the full history of this investigation if it matters for your use case
