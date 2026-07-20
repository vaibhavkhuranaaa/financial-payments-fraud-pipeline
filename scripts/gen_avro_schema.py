"""Generate contracts/transaction.avsc from contracts/transaction.schema.json.

ADR 0006 (decision 3): the JSON-Schema contract stays the human source of
truth; the Avro schema is *derived* from it by this script so the two can
never drift silently. `--check` regenerates in memory and diffs against the
committed .avsc — that's the CI gate (see Makefile target `avro-schema-check`).

Mapping (ADR 0006 decision 4):
  - record name "Transaction", fixed namespace (below) — stable identifier,
    doesn't need to resolve to anything.
  - fields keep the contract file's property order.
  - required properties -> the mapped scalar type, non-null.
  - the three optional properties (zip, errors, is_fraud) -> ["null", T]
    with "default": null.
  - string -> string, number -> double, integer -> int, boolean -> boolean.
  - JSON-Schema `enum` (channel) -> Avro `string`, NOT an Avro enum: Avro
    enum evolution is a well-known trap, and the enum's values are already
    enforced upstream by JSON-Schema validation before serialization.
  - `description` -> `doc` when present.

Output is byte-stable across runs: fixed field-dict key order, indent=2,
trailing newline, no key sorting beyond what the code itself builds.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SCHEMA_PATH = os.path.join(_REPO_ROOT, "contracts", "transaction.schema.json")
DEFAULT_OUTPUT_PATH = os.path.join(_REPO_ROOT, "contracts", "transaction.avsc")

# Stable, does not need to resolve — see module docstring.
NAMESPACE = "com.fraudpipeline.contracts"
RECORD_NAME = "Transaction"

_PRIMITIVE_MAP = {
    "string": "string",
    "number": "double",
    "integer": "int",
    "boolean": "boolean",
}


def load_contract(path: str = DEFAULT_SCHEMA_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _base_type(name: str, prop: dict[str, Any]) -> str:
    """Resolve a JSON-Schema property to its scalar Avro type name."""
    if "enum" in prop:
        return "string"
    if "const" in prop:
        const_val = prop["const"]
        if isinstance(const_val, bool):
            return "boolean"
        if isinstance(const_val, str):
            return "string"
        if isinstance(const_val, int):
            return "int"
        if isinstance(const_val, float):
            return "double"
        raise ValueError(f"{name}: unsupported const type {type(const_val)!r}")
    if "type" not in prop:
        raise ValueError(f"{name}: property has neither 'type', 'enum', nor 'const'")
    json_type = prop["type"]
    types = [json_type] if isinstance(json_type, str) else list(json_type)
    non_null = [t for t in types if t != "null"]
    if len(non_null) != 1:
        raise ValueError(f"{name}: expected exactly one non-null type, got {types!r}")
    try:
        return _PRIMITIVE_MAP[non_null[0]]
    except KeyError:
        raise ValueError(f"{name}: unmapped JSON-Schema type {non_null[0]!r}") from None


def build_avro_schema(contract: dict[str, Any]) -> dict[str, Any]:
    """Build the Avro record dict for `contract` (a loaded JSON-Schema doc)."""
    required = set(contract.get("required", []))
    fields: list[dict[str, Any]] = []
    for name, prop in contract["properties"].items():
        base = _base_type(name, prop)
        field: dict[str, Any] = {"name": name}
        if name in required:
            field["type"] = base
        else:
            field["type"] = ["null", base]
        if "description" in prop:
            field["doc"] = prop["description"]
        if name not in required:
            field["default"] = None
        fields.append(field)

    avro_schema: dict[str, Any] = {
        "type": "record",
        "name": RECORD_NAME,
        "namespace": NAMESPACE,
    }
    if "description" in contract:
        avro_schema["doc"] = contract["description"]
    avro_schema["fields"] = fields
    return avro_schema


def render(avro_schema: dict[str, Any]) -> str:
    return json.dumps(avro_schema, indent=2) + "\n"


def generate(schema_path: str = DEFAULT_SCHEMA_PATH) -> str:
    """Load `schema_path` and render its Avro schema text (used by tests)."""
    return render(build_avro_schema(load_contract(schema_path)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA_PATH, help="input JSON-Schema contract path")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="output .avsc path")
    parser.add_argument(
        "--check",
        action="store_true",
        help="regenerate in memory and diff against --output; exit non-zero on mismatch",
    )
    args = parser.parse_args(argv)

    rendered = generate(args.schema)

    if args.check:
        if not os.path.exists(args.output):
            print(f"gen_avro_schema --check: {args.output} does not exist — run without --check to generate it", file=sys.stderr)
            return 1
        with open(args.output, encoding="utf-8") as f:
            existing = f.read()
        if existing != rendered:
            print(
                f"gen_avro_schema --check: {args.output} is out of sync with {args.schema}\n"
                "Run `python scripts/gen_avro_schema.py` to regenerate it.",
                file=sys.stderr,
            )
            return 1
        print(f"gen_avro_schema --check: {args.output} is in sync with {args.schema}")
        return 0

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(rendered)
    print(f"gen_avro_schema: wrote {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
