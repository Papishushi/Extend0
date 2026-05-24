# Ops Notes

## Operational Intent

`abox-doctor.py` is a low-risk diagnostic helper for this phase. It should remain read-only by default and report unsupported operations explicitly.

## Current Safe Uses

- inspect whether the current ontology foundation is present
- emit findings as JSON
- generate a fix-document skeleton for later human or tool review

## Unsupported Operations

- direct ontology mutation
- automatic semantic repair
- destructive cleanup of ontology files
