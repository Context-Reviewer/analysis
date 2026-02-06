# Next Milestone: Make-Like Orchestrator

**Scope (one-liner):**
> Implement a make-like orchestrator that only validates config and runs existing steps in order, producing byte-identical `docs/data/report.json` to the manual run, with no schema or metric changes.

---

## Pre-Implementation Checklist

- [ ] Commit all current changes
- [ ] Tag freeze point: `v0.2.0-post-dashboard-live-stable`
- [ ] Push tag to origin
- [ ] Verify GitHub Pages still works

```powershell
cd C:\dev\repos\analysis
git status --short  # must be empty or only report.json

git add -A
git commit -m "feat: topic categorization v1 + Step 4 report generator"
git tag v0.2.0-post-dashboard-live-stable
git push origin main --tags
```

---

## Implementation Constraints

- Read `ORCHESTRATOR_SPEC.md` — that is the contract
- No inference
- No derived fields  
- No schema changes
- Must pass byte-identical verification test
- Fail loud on any validation or step failure

---

## Files to Create

| File | Purpose |
|------|---------|
| `config/subjects/sean_roy.json` | First subject config |
| `tools/orchestrate.py` | Orchestrator entrypoint |

---

## Done When

1. `python tools/orchestrate.py --config config\subjects\sean_roy.json` runs successfully
2. `docs/data/report.json` is byte-identical to manual run
3. Config validation fails loud on bad input
4. Step skip works correctly for null `comments_jsonl`
