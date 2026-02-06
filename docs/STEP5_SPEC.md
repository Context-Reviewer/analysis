# Step 5 Acceptance Specification: Analysis Enrichment

**Status:** Locked contract. Implementation must match exactly.

---

## 1️⃣ Scope & Position in Pipeline

Step 5 runs **after** Step 4 (`generate_report_json.py`).

```
step1 → step1b → step2 → step3 → step4 → step5
```

Step 5 produces two **additive** enrichments to `report.json`:
1. `self_portrayal`
2. `cross_topic_intrusion`

Step 5 **must not** modify or re-interpret any existing fields in `report.json`.

---

## 2️⃣ Inputs

### Allowed Inputs

Step 5 may read:
- `docs/data/report.json` (Step 4 output)
- Normalized items file in `paths.intermediate_dir` (as produced by Step 1)
- Topic assignments file in `paths.intermediate_dir` (as produced by Step 3)
- Timeline data file in `paths.intermediate_dir` (as produced by Step 2)

File names are determined by the active run configuration, not hardcoded.

### Forbidden Inputs

Step 5 **must not** read:
- Raw HTML or scraper artifacts
- Pre-normalization data (`posts_all.jsonl` or equivalent)
- UI state or browser data
- External APIs or network data
- Previous Step 5 outputs (no self-reference)
- Any files not listed in "Allowed Inputs"

---

## 3️⃣ Outputs

### Output Location

Step 5 writes to a **new file**: `docs/data/report.step5.json`

Step 4 output (`docs/data/report.json`) remains **untouched and byte-identical**.

### Output Content

The output file contains:
- All fields from Step 4 `report.json` (copied unchanged)
- `self_portrayal` field (added)
- `cross_topic_intrusion` field (added)

### Output Rules

| Rule | Requirement |
|------|-------------|
| Fields are additive | Existing fields copied unchanged from Step 4 |
| Null or object | Each new field is either `null` or a complete object |
| No partial objects | If computed, object must contain all required fields |
| No empty objects | `{}` must not substitute for `null` |
| Schema version | Each object must include its `schema_version` |

### Backward Compatibility

If Step 5 is **disabled** or **not run**:
- `report.json` remains **byte-identical** to Step 4 output
- `report.step5.json` does not exist (or is not updated)
- Dashboard may consume either file based on availability

---

## 4️⃣ Self-Portrayal Acceptance Rules

### What Qualifies as a Self-Portrayal Claim

A claim qualifies if:
- It is **first-person** ("I", "I'm", "I've", "my")
- It is **explicit** (stated directly, not implied)
- It describes **how the speaker frames themselves**
- It can be quoted **verbatim**

### What Disqualifies a Claim

A claim is disqualified if:
- It is third-person or general
- It requires interpretation to extract
- It is biographical fact without self-framing
- It is hypothetical or conditional

### Counting Rules

- Each distinct claim is counted once
- Same claim in multiple items: count each occurrence
- `total_claims` must equal `sum(categories.*)`

### Category Assignment Rules

Each claim must be assigned to exactly one category from the fixed set:
- `moral_identity`
- `victimhood`
- `authority_expertise`
- `care_empathy`
- `ingroup_outgroup_positioning`
- `personal_responsibility`
- `other`

Assignment is based on **framing**, not fact. Categories describe **rhetorical positioning**.

### Example Inclusion Rules

- Examples must be verbatim excerpts
- Examples must be non-empty strings
- Each example must include: `item_id`, `timestamp` (or null), `topic`, `claim_category`, `excerpt`
- Examples are representative, not exhaustive

### Explicit Prohibitions

- Claims are **quoted, not interpreted**
- No assessment of claim validity or truth
- No biographical categorization
- No sensitive attribute inference

---

## 5️⃣ Cross-Topic Intrusion Acceptance Rules

### Definition of a Thread

A **thread** is a single post with all its replies, treated as one unit.

### Source vs Target Determination

| Term | Definition |
|------|------------|
| Source topic | The primary topic of the thread (determined by root post) |
| Target topic | A different topic that appears in thread replies |

Intrusion is **directional**: source → target.

### Counting Rules

- `count`: Number of threads with source topic that contain target topic
- A thread may contribute to multiple pairs if it contains multiple target topics
- Only non-zero counts appear in output (sparse)

### Denominator Rules

- `denominator`: Total threads with the source topic
- `denominator` must be positive (> 0)
- `count` must be ≤ `denominator`

### Rate Calculation

```
rate = count / denominator
```

- Full precision (no rounding)
- Verification tolerance: `abs(rate - count/denominator) < 1e-9`

### Sparsity Requirements

- Only pairs with `count > 0` appear
- Empty `pairs` array is valid (no intrusion detected)

### Directionality Rules

- `A → B` is independent of `B → A`
- No symmetric assumption
- Each direction computed separately

### Explicit Prohibitions

- No symmetric matrices
- No global intrusion rate
- No item-level co-occurrence metrics
- No aggregate "intrusion score"

---

## 6️⃣ Determinism & Reproducibility

### Ordering Guarantees

- `pairs` array: sorted by `(source, target)` lexicographically
- `examples` array: sorted by `item_id` lexicographically
- `categories` object: keys in fixed order (as defined in schema)

### Floating-Point Tolerance

- Rate comparison tolerance: `1e-9`
- Generator must output full precision
- No rounding in stored values

### Stable Iteration

- All iterations over items, topics, threads must be deterministic
- Same inputs must produce identical outputs

### Identical Output Conditions

Step 5 outputs must be byte-identical across runs when:
- Input files are identical
- No external state is consulted
- Execution environment does not affect output

---

## 7️⃣ Failure Modes

### Fail-Loud Conditions (Exit ≠ 0)

| Condition | Action |
|-----------|--------|
| Schema version mismatch in input | Exit 1 |
| Invalid claim category (not in fixed set) | Exit 1 |
| Math invariant violation (`total_claims ≠ sum`) | Exit 1 |
| Rate invariant violation | Exit 1 |
| Missing required upstream file | Exit 1 |
| `count > denominator` | Exit 1 |

### Non-Failure Conditions (Valid States)

| Condition | Result |
|-----------|--------|
| Zero self-portrayal claims | `total_claims: 0`, empty categories |
| No intrusion pairs | `pairs: []` |
| All categories are zero | Valid |
| Step 5 output is all-null | Valid |

---

## 8️⃣ Verification Criteria

### Test 1: Step 4 Output Unchanged

```powershell
# Capture Step 4 hash before Step 5
(Get-FileHash docs\data\report.json).Hash | Out-File step4.sha256 -Encoding ascii

# Run Step 5
python tools/step5_analyze_enrichment.py

# Step 4 output must be byte-identical
(Get-FileHash docs\data\report.json).Hash | Out-File step4_after.sha256 -Encoding ascii
Compare-Object (Get-Content step4.sha256) (Get-Content step4_after.sha256)  # Must show no difference
```

### Test 2: Step 5 Output Exists and Valid

```powershell
# Verify Step 5 output file exists
Test-Path docs\data\report.step5.json  # Must be True

# Verify it contains self_portrayal and cross_topic_intrusion fields
$json = Get-Content docs\data\report.step5.json | ConvertFrom-Json
$json.PSObject.Properties.Name -contains 'self_portrayal'  # Must be True
$json.PSObject.Properties.Name -contains 'cross_topic_intrusion'  # Must be True
```

### Test 3: Deterministic Reruns

```powershell
# Run Step 5 twice
python tools/step5_analyze_enrichment.py
(Get-FileHash docs\data\report.step5.json).Hash | Out-File run1.sha256 -Encoding ascii

python tools/step5_analyze_enrichment.py
(Get-FileHash docs\data\report.step5.json).Hash | Out-File run2.sha256 -Encoding ascii

# Must be identical
Compare-Object (Get-Content run1.sha256) (Get-Content run2.sha256)
```

---

## 9️⃣ Non-Goals (Explicit)

Step 5 **must not** perform or enable:

| Forbidden | Reason |
|-----------|--------|
| Virtue signaling detection | Implies motive |
| Hypocrisy labeling | Requires judgment |
| Bad faith inference | Cannot be observed |
| Motive attribution | Intent cannot be sourced |
| Agenda inference | Psychological |
| Personality trait detection | Profiling |
| Scoring / ranking / grading | Evaluative |
| Normative labels | "excessive", "problematic" |
| Comparative judgment | "more than average", "unusually" |
| Biographical categorization | Describes framing, not facts |
| Truth assessment | No claim validity checking |
| Sensitive attribute inference | No demographic profiling |

---

## Summary

| Component | Schema | Status |
|-----------|--------|--------|
| `self_portrayal` | `self_portrayal_v1` | Locked |
| `cross_topic_intrusion` | `cross_topic_intrusion_v1` | Locked |

Step 5 is **additive, optional, and deterministic**.

---

Awaiting review.
