from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TBOX_PATH = ROOT / "ontology" / "tbox" / "extend0.owl"
DEFAULT_ABOX_PATH = ROOT / "ontology" / "abox" / "example-abox.ttl"

OWL_NS = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
}

URI_PREFIXES = {
    "https://extend0.se777en.fyi/ns#": "ns",
    "https://extend0.se777en.fyi/abox#": "ex",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf",
    "http://www.w3.org/2000/01/rdf-schema#": "rdfs",
    "http://www.w3.org/2002/07/owl#": "owl",
}


@dataclass(frozen=True)
class QueryGraph:
    classes: list[dict]
    properties: list[dict]
    individuals: list[dict]
    triples: list[tuple[str, str, str]]


def slugify(value: str) -> str:
    lowered = value.strip().lower()
    replaced = re.sub(r"[^a-z0-9]+", "-", lowered)
    return replaced.strip("-")


def _json(payload: object) -> str:
    return json.dumps(payload, indent=2, sort_keys=True)


def _qname(uri_or_fragment: str) -> str:
    if uri_or_fragment.startswith("#"):
        return f"ns:{uri_or_fragment[1:]}"
    if ":" in uri_or_fragment and not uri_or_fragment.startswith("http"):
        return uri_or_fragment
    for base, prefix in URI_PREFIXES.items():
        if uri_or_fragment.startswith(base):
            return f"{prefix}:{uri_or_fragment[len(base):]}"
    return uri_or_fragment


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _child_texts(node: ET.Element, suffix: str) -> list[str]:
    values: list[str] = []
    for child in node:
        if child.tag.endswith(suffix) and child.text:
            values.append(child.text.strip())
    return values


def _child_resource_values(node: ET.Element, suffix: str) -> list[str]:
    values: list[str] = []
    rdf_resource = f"{{{OWL_NS['rdf']}}}resource"
    for child in node:
        if child.tag.endswith(suffix):
            resource = child.attrib.get(rdf_resource)
            if resource:
                values.append(_qname(resource))
    return values


def _parse_tbox() -> tuple[list[dict], list[dict], list[dict], list[tuple[str, str, str]]]:
    root = ET.fromstring(_read_text(TBOX_PATH))
    classes: list[dict] = []
    properties: list[dict] = []
    individuals: list[dict] = []
    triples: list[tuple[str, str, str]] = []
    rdf_about = f"{{{OWL_NS['rdf']}}}about"

    for node in root.findall("owl:Class", OWL_NS):
        about = node.attrib.get(rdf_about, "")
        if not about.startswith("#"):
            continue
        identifier = _qname(about)
        entry = {
            "id": identifier,
            "name": identifier.split(":", 1)[1],
            "labels": _child_texts(node, "label"),
            "comments": _child_texts(node, "comment"),
            "subClassOf": _child_resource_values(node, "subClassOf"),
            "mapsToCodeSymbol": _child_texts(node, "mapsToCodeSymbol"),
        }
        classes.append(entry)
        triples.append((identifier, "rdf:type", "owl:Class"))
        for base in entry["subClassOf"]:
            triples.append((identifier, "rdfs:subClassOf", base))
        for label in entry["labels"]:
            triples.append((identifier, "rdfs:label", label))
        for comment in entry["comments"]:
            triples.append((identifier, "rdfs:comment", comment))

    for kind in ("ObjectProperty", "DatatypeProperty", "AnnotationProperty"):
        for node in root.findall(f"owl:{kind}", OWL_NS):
            about = node.attrib.get(rdf_about, "")
            if not about.startswith("#"):
                continue
            identifier = _qname(about)
            entry = {
                "id": identifier,
                "name": identifier.split(":", 1)[1],
                "kind": kind,
                "labels": _child_texts(node, "label"),
                "comments": _child_texts(node, "comment"),
                "domain": _child_resource_values(node, "domain"),
                "range": _child_resource_values(node, "range"),
            }
            properties.append(entry)
            triples.append((identifier, "rdf:type", f"owl:{kind}"))
            for label in entry["labels"]:
                triples.append((identifier, "rdfs:label", label))
            for domain in entry["domain"]:
                triples.append((identifier, "rdfs:domain", domain))
            for range_value in entry["range"]:
                triples.append((identifier, "rdfs:range", range_value))

    for node in root.findall("owl:NamedIndividual", OWL_NS):
        about = node.attrib.get(rdf_about, "")
        if not about.startswith("#"):
            continue
        identifier = _qname(about)
        types = _child_resource_values(node, "type")
        entry = {
            "id": identifier,
            "name": identifier.split(":", 1)[1],
            "labels": _child_texts(node, "label"),
            "types": types,
        }
        individuals.append(entry)
        triples.append((identifier, "rdf:type", "owl:NamedIndividual"))
        for type_name in types:
            triples.append((identifier, "rdf:type", type_name))

    classes.sort(key=lambda item: item["id"])
    properties.sort(key=lambda item: item["id"])
    individuals.sort(key=lambda item: item["id"])
    return classes, properties, individuals, triples


def _parse_turtle(text: str) -> tuple[list[dict], list[tuple[str, str, str]]]:
    prefixes: dict[str, str] = {}
    statements: list[str] = []
    current: list[str] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("@prefix"):
            match = re.match(r"@prefix\s+([A-Za-z0-9_-]+):\s+<([^>]+)>\s*\.", line)
            if match:
                prefixes[match.group(1)] = match.group(2)
            continue
        current.append(line)
        if line.endswith("."):
            statements.append(" ".join(current))
            current = []

    individuals: dict[str, dict] = {}
    triples: list[tuple[str, str, str]] = []

    def ensure_individual(identifier: str) -> dict:
        if identifier not in individuals:
            individuals[identifier] = {"id": identifier, "name": identifier.split(":", 1)[1], "types": []}
        return individuals[identifier]

    for statement in statements:
        body = statement[:-1].strip()
        if not body:
            continue
        subject, remainder = body.split(" ", 1)
        ensure_individual(subject)
        for clause in [part.strip() for part in remainder.split(";") if part.strip()]:
            predicate, object_part = clause.split(" ", 1)
            objects = [item.strip() for item in object_part.split(",") if item.strip()]
            predicate_name = "rdf:type" if predicate == "a" else predicate
            for obj in objects:
                triples.append((subject, predicate_name, obj))
                if predicate == "a":
                    ensure_individual(subject)["types"].append(obj)

    return sorted(individuals.values(), key=lambda item: item["id"]), triples


def load_graph(abox_paths: list[str] | None = None) -> QueryGraph:
    classes, properties, individuals, triples = _parse_tbox()
    for path_str in abox_paths or [str(DEFAULT_ABOX_PATH)]:
        path = Path(path_str)
        abox_individuals, abox_triples = _parse_turtle(_read_text(path))
        individuals.extend(abox_individuals)
        triples.extend(abox_triples)
    merged: dict[str, dict] = {}
    for individual in individuals:
        existing = merged.setdefault(individual["id"], {"id": individual["id"], "name": individual["name"], "types": []})
        for type_name in individual.get("types", []):
            if type_name not in existing["types"]:
                existing["types"].append(type_name)
    return QueryGraph(
        classes=classes,
        properties=properties,
        individuals=sorted(merged.values(), key=lambda item: item["id"]),
        triples=triples,
    )


def classes_json(abox_paths: list[str] | None = None) -> str:
    graph = load_graph(abox_paths)
    return _json({"classes": graph.classes})


def class_json(name: str, abox_paths: list[str] | None = None) -> str:
    graph = load_graph(abox_paths)
    target = name if name.startswith("ns:") else f"ns:{name}"
    for entry in graph.classes:
        if entry["id"] == target or entry["name"] == name:
            subclasses = sorted(item["id"] for item in graph.classes if target in item["subClassOf"])
            properties = sorted(item["id"] for item in graph.properties if target in item["domain"] or target in item["range"])
            return _json({"class": entry, "subclasses": subclasses, "relatedProperties": properties})
    return _json({"error": f"class not found: {name}"})


def properties_json(abox_paths: list[str] | None = None) -> str:
    graph = load_graph(abox_paths)
    return _json({"properties": graph.properties})


def individuals_json(abox_paths: list[str] | None = None) -> str:
    graph = load_graph(abox_paths)
    return _json({"individuals": graph.individuals})


def find_json(term: str, abox_paths: list[str] | None = None) -> str:
    graph = load_graph(abox_paths)
    needle = term.lower()
    results: list[dict] = []

    for entry in graph.classes:
        haystacks = [entry["id"], entry["name"], *entry["labels"], *entry["comments"], *entry["mapsToCodeSymbol"]]
        if any(needle in item.lower() for item in haystacks):
            results.append({"kind": "class", "id": entry["id"], "name": entry["name"]})

    for entry in graph.properties:
        haystacks = [entry["id"], entry["name"], *entry["labels"], *entry["comments"]]
        if any(needle in item.lower() for item in haystacks):
            results.append({"kind": "property", "id": entry["id"], "name": entry["name"]})

    for entry in graph.individuals:
        haystacks = [entry["id"], entry["name"], *entry.get("types", [])]
        if any(needle in item.lower() for item in haystacks):
            results.append({"kind": "individual", "id": entry["id"], "name": entry["name"], "types": entry.get("types", [])})

    return _json({"query": term, "results": results})


def _match_term(term: str, triple_value: str, binding: dict[str, str]) -> dict[str, str] | None:
    if term.startswith("?"):
        current = binding.get(term)
        if current is None:
            updated = dict(binding)
            updated[term] = triple_value
            return updated
        return binding if current == triple_value else None
    expected = "rdf:type" if term == "a" else term
    return binding if expected == triple_value else None


def _evaluate_select(query: str, triples: list[tuple[str, str, str]]) -> dict:
    normalized = " ".join(query.strip().split())
    match = re.match(r"SELECT\s+(?P<select>.+?)\s+WHERE\s*\{(?P<body>.+)\}\s*$", normalized, re.IGNORECASE)
    if not match:
        return {"error": "Only basic SELECT ... WHERE { ... } queries are supported."}

    select_vars = [token for token in match.group("select").split() if token.startswith("?")]
    body = match.group("body").strip()
    if body.endswith("."):
        body = body[:-1].strip()
    raw_patterns = [segment.strip() for segment in re.split(r"\s+\.\s+", body) if segment.strip()]
    patterns: list[tuple[str, str, str]] = []
    for raw in raw_patterns:
        tokens = raw.split()
        if len(tokens) != 3:
            return {"error": f"Unsupported triple pattern: {raw}"}
        patterns.append((tokens[0], tokens[1], tokens[2]))

    bindings: list[dict[str, str]] = [{}]
    for subject_term, predicate_term, object_term in patterns:
        next_bindings: list[dict[str, str]] = []
        for binding in bindings:
            for triple in triples:
                s_value, p_value, o_value = triple
                bound = _match_term(subject_term, s_value, binding)
                if bound is None:
                    continue
                bound = _match_term(predicate_term, p_value, bound)
                if bound is None:
                    continue
                bound = _match_term(object_term, o_value, bound)
                if bound is None:
                    continue
                next_bindings.append(bound)
        bindings = next_bindings

    rows = [{var[1:]: binding.get(var) for var in select_vars} for binding in bindings]
    return {"query": normalized, "rows": rows}


def sparql_json(query: str, abox_paths: list[str] | None = None) -> str:
    graph = load_graph(abox_paths)
    return _json(_evaluate_select(query, graph.triples))
