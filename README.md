# FORGE-VA: Forensic Oversight Risk and Gaming Evaluation for Veterans Affairs

**A decision support framework for detecting gaming behavior in healthcare performance measurement systems.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Overview

FORGE-VA is a multi-block anomaly detection framework designed to identify statistically implausible performance patterns indicative of gaming behavior at the facility level in large-scale public healthcare systems. The framework integrates two independent VA data sources — SAIL and VAC3 — into a unified, decomposable architecture that produces interpretable risk scores for audit prioritization.

The framework is described in full in:

> Sharma, A., Fulton, C., Tomic, A., & Fulton, L. (2025). *Identifying Structural Vulnerabilities in Healthcare Performance Measurement: A Decision Support Framework for Detecting Gaming Behavior.* Submitted to *Decision Support Systems*.

---

## Repository Structure

```
VA/
├── SAIL_Extraction2_fixed.py              # OIG URL scanner and PDF discovery
├── FORGE VA Supplementary Variables.pdf   # Identifier consolidation, metric harmonization, operational definitions, feature list, and Isolation Forest configuration
│
└── [OIG-Reports branch]
    ├── ground_truth_final.csv             # Facility-level OIG classifications
    ├── discovered_urls.json               # Confirmed OIG report URLs (14-02890 series)
    └── [PDF corpus]                       # Downloaded VA OIG administrative summaries
```

### Branch: `OIG-Reports`

The `OIG-Reports` branch contains the complete VA OIG wait-time investigation corpus used as external ground truth in the FORGE-VA validation framework.

- **`ground_truth_final.csv`** — Facility-to-report mapping with classifications: `Substantiated (S)`, `Not Substantiated (N)`, and `Ambiguous (A)`
- **`discovered_urls.json`** — All confirmed PDF URLs discovered via systematic enumeration of the VA OIG public document repository
- **PDF reports** — Full text of VA OIG Administrative Investigation Summaries, 14-02890 series (2014–2016)

Classification procedure: each report was independently reviewed and classified based on the OIG's stated conclusions. Where multiple reports addressed the same facility catchment area, the most severe classification was retained, consistent with a conservative forensic stance.

---

## Data Sources

All data used in this study are publicly available:

| Source | Description | Access |
|--------|-------------|--------|
| SAIL | Strategic Analytics for Improvement and Learning, FY2015–FY2024 | [data.va.gov](https://data.va.gov) |
| VAC3 | VA Centralized Clinical and Administrative Clarity, FY2017–FY2024 | [data.va.gov](https://data.va.gov) |
| VA OIG Reports | Administrative investigation summaries, 14-02890 series | [vaoig.gov/reports/all](https://www.vaoig.gov/reports/all) |

---

## OIG Report Discovery

The script `SAIL_Extraction2_fixed.py` performs systematic URL enumeration of the VA OIG public document repository, targeting the confirmed 14-02890 series pattern across three release paths:

- `2016-02` — February 2016 releases
- `2016-03` — March 2016 releases  
- `2015-09` — September 2015 releases

Suffix numbers are scanned exhaustively over the range `[0, 999]`. Only URLs returning HTTP 200 with valid PDF content are retained. Results are written to `oig_reports/discovered_urls.json`.

```bash
python SAIL_Extraction2_fixed.py
```

Note: The Phoenix VA Health Care System report (14-02603-267, August 2014) and reports outside the three date paths above were obtained directly and are included in the corpus separately.

---

## FORGE-VA Architecture

The framework proceeds in three stages:

**Block 1 — SAIL Outcome Signal**
Direction-adjusted standardized performance scores aggregated to facility level across gaming-susceptible measure categories (ED throughput, hospital flow, length of stay, care transitions, patient experience, outpatient performance, prevention composites, efficiency).

**Block 2 — VAC3 Behavioral Signal**
Engineered behavioral anomaly features including threshold bunching, coordinated improvement, Mahalanobis distance, Isolation Forest anomaly scores, and temporal consistency measures.

**Block 3 — Cross-System Coupling Term**
Multiplicative rank product of Block 1 and Block 2 percentile ranks. Elevated only when a facility ranks high on both outcome extremity and behavioral anomaly simultaneously.

**Ensemble Score**
Effect-size-weighted (rank-biserial $r$) fusion of all three blocks. Facilities are ranked in descending order of ensemble score for audit prioritization.

---

## Validation

External ground truth: VA OIG administrative investigation summaries classified as:
- **S** — Substantiated (confirmed fraud, $n = 31$)
- **N** — Not Substantiated (investigated, $n = 19$)
- **B** — Background (never investigated, $n = 72$)

| Metric | Value |
|--------|-------|
| AUC-ROC (Ensemble) | 0.748 |
| PR-AUC | 0.527 |
| Fraud Recall | 0.677 |
| Enrichment vs. Random | ~2× |
| S > B separation | p < 0.0001 |
| N vs. B separation | p = 0.2215 |

---

## Citation

If you use this code or corpus in your research, please cite:

```bibtex
@misc{fulton2026oig_corpus,
  author       = {Fulton, Lawrence},
  title        = {{VA OIG Wait-Time Investigation Report Corpus: 
                   Facility Classifications for FORGE-VA Validation}},
  year         = {2026},
  howpublished = {GitHub repository, branch OIG-Reports},
  url          = {https://github.com/dustoff06/VA/tree/OIG-Reports},
  note         = {{Contains full report inventory, facility-to-report 
                   mapping, and Substantiated / Not Substantiated / 
                   Ambiguous classifications used in the FORGE-VA 
                   validation sample}}
}
```

---

## Authors

- **Arvind Sharma** — Woods College of Advancing Studies, Boston College
- **Christopher Fulton** — Air Force Institute of Technology
- **Aleksandar Tomic** — Woods College of Advancing Studies, Boston College
- **Lawrence Fulton** — Woods College of Advancing Studies, Boston College

Corresponding author: [fulton@bc.edu](mailto:fulton@bc.edu)

---

## License

This repository is licensed under the MIT License. OIG report PDFs are public domain U.S. government documents. See [LICENSE](LICENSE) for details.
