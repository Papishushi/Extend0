# Ontology Query Skill

Use this skill when the question is about Extend0 domain meaning, system boundaries, architectural vocabulary, or ontology-backed consistency rather than raw implementation mechanics.

## Rule

Consult the ontology before deep code exploration when the task is primarily about:

- what a subsystem means
- how systems relate
- which terms are canonical
- whether a concept belongs to the domain core or to a demo/example

## Canonical Commands

Run from the repository root:

```bash
python ontology/skills/ontology-query/query.py classes
python ontology/skills/ontology-query/query.py class MetaDBSystem
python ontology/skills/ontology-query/query.py properties
python ontology/skills/ontology-query/query.py individuals
python ontology/skills/ontology-query/query.py find transport
python ontology/skills/ontology-query/query.py sparql "SELECT ?c WHERE { ?c rdf:type owl:Class . }"
```

You can add one or more ABox files:

```bash
python ontology/skills/ontology-query/query.py --abox ontology/abox/example-abox.ttl individuals
```

## Supported SPARQL

The current implementation supports a lightweight subset:

- `SELECT ... WHERE { ... }`
- triple patterns only
- variables like `?x`
- qnames such as `ns:MetaDBSystem`, `ex:metadb-access`, `rdf:type`, `rdfs:subClassOf`, `owl:Class`
- `a` as shorthand for `rdf:type`

It does not yet support filters, optionals, prefixes declared inside the query, aggregates, or full SPARQL syntax.

## Main Extend0 Concepts

- `Extend0Platform`
- `LifecycleSystem`
- `MetaDBSystem`
- `Transport`
- `ServiceIdentity`
- `AccessSurface`
- `OwnershipClaim`
- `Lease`
- `HeartbeatSignal`

## Usage Guidance

- Use the ontology to stabilize language first.
- Then verify mismatches against code and docs.
- Treat demo-specific concepts as suspicious unless they exist in accepted ADRs or stable public contracts.
