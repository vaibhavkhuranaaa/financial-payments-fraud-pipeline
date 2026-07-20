"""Unit tests for scripts/gen_avro_schema.py: the JSON-Schema -> Avro
converter for contracts/transaction.schema.json (ADR 0006).

Hermetic: no registry, no broker. Exercises the converter's pure functions
directly, its --check CLI mode against tmp-dir copies, and round-trips a
sample event through fastavro against the generated schema.
"""

from __future__ import annotations

import io
import json

import fastavro
import pytest

from scripts import gen_avro_schema as gen

CONTRACT_PATH = gen.DEFAULT_SCHEMA_PATH
COMMITTED_AVSC_PATH = gen.DEFAULT_OUTPUT_PATH


def _fields_by_name(avro_schema: dict) -> dict[str, dict]:
    return {f["name"]: f for f in avro_schema["fields"]}


@pytest.fixture(scope="module")
def contract() -> dict:
    return gen.load_contract(CONTRACT_PATH)


@pytest.fixture(scope="module")
def avro_schema(contract: dict) -> dict:
    return gen.build_avro_schema(contract)


class TestTypeMapping:
    def test_record_name_and_field_order_match_contract(self, contract, avro_schema):
        assert avro_schema["type"] == "record"
        assert avro_schema["name"] == "Transaction"
        assert avro_schema["fields"][0]["name"] != ""
        names = [f["name"] for f in avro_schema["fields"]]
        assert names == list(contract["properties"].keys())

    def test_scalar_type_mapping(self, avro_schema):
        fields = _fields_by_name(avro_schema)
        assert fields["event_id"]["type"] == "string"
        assert fields["amount"]["type"] == "double"
        assert fields["mcc"]["type"] == "int"
        # is_fraud is nullable in the contract, so its base type is nested
        # inside the union rather than asserted here directly.

    def test_boolean_field_maps_to_boolean_inside_its_union(self, avro_schema):
        fields = _fields_by_name(avro_schema)
        assert fields["is_fraud"]["type"] == ["null", "boolean"]

    def test_enum_maps_to_string_not_avro_enum(self, contract, avro_schema):
        # channel is a JSON-Schema enum in the contract...
        assert "enum" in contract["properties"]["channel"]
        # ...but ADR 0006 decision 4: Avro string, not an Avro enum symbol type.
        fields = _fields_by_name(avro_schema)
        assert fields["channel"]["type"] == "string"

    def test_const_field_maps_to_string(self, avro_schema):
        fields = _fields_by_name(avro_schema)
        assert fields["schema_version"]["type"] == "string"

    def test_required_fields_are_non_null(self, contract, avro_schema):
        fields = _fields_by_name(avro_schema)
        for name in contract["required"]:
            assert fields[name]["type"] != "null"
            assert not (isinstance(fields[name]["type"], list) and "null" in fields[name]["type"])
            assert "default" not in fields[name]

    def test_nullable_optionals_are_null_unions_with_null_default(self, contract, avro_schema):
        fields = _fields_by_name(avro_schema)
        optional = set(contract["properties"]) - set(contract["required"])
        assert optional == {"zip", "errors", "is_fraud"}
        for name in optional:
            field = fields[name]
            assert isinstance(field["type"], list)
            assert field["type"][0] == "null"
            assert field["default"] is None

    def test_description_carried_as_doc_when_present(self, contract, avro_schema):
        fields = _fields_by_name(avro_schema)
        assert fields["amount"]["doc"] == contract["properties"]["amount"]["description"]
        # merchant_city has no description in the contract -> no doc key.
        assert "description" not in contract["properties"]["merchant_city"]
        assert "doc" not in fields["merchant_city"]


class TestDeterminism:
    def test_two_runs_are_byte_identical(self):
        first = gen.generate(CONTRACT_PATH)
        second = gen.generate(CONTRACT_PATH)
        assert first == second

    def test_output_ends_with_single_trailing_newline(self):
        rendered = gen.generate(CONTRACT_PATH)
        assert rendered.endswith("\n")
        assert not rendered.endswith("\n\n")


class TestCheckMode:
    def test_check_passes_on_committed_file(self):
        assert gen.main(["--schema", CONTRACT_PATH, "--output", COMMITTED_AVSC_PATH, "--check"]) == 0

    def test_check_fails_on_tampered_copy(self, tmp_path, capsys):
        tampered = tmp_path / "transaction.avsc"
        tampered.write_text('{"type": "record", "name": "NotTransaction", "fields": []}\n')
        rc = gen.main(["--schema", CONTRACT_PATH, "--output", str(tampered), "--check"])
        assert rc != 0
        err = capsys.readouterr().err
        assert "out of sync" in err

    def test_check_fails_when_output_missing(self, tmp_path, capsys):
        missing = tmp_path / "does-not-exist.avsc"
        rc = gen.main(["--schema", CONTRACT_PATH, "--output", str(missing), "--check"])
        assert rc != 0

    def test_generated_file_matches_committed_file(self, tmp_path):
        out = tmp_path / "transaction.avsc"
        rc = gen.main(["--schema", CONTRACT_PATH, "--output", str(out)])
        assert rc == 0
        with open(COMMITTED_AVSC_PATH, encoding="utf-8") as f:
            committed = f.read()
        assert out.read_text() == committed


class TestFastavroCompatibility:
    def test_parse_schema_accepts_generated_schema(self, avro_schema):
        parsed = fastavro.parse_schema(avro_schema)
        assert parsed is not None

    def _sample_event(self) -> dict:
        return {
            "schema_version": "1.0.0",
            "event_id": "b2b1c1a0-1111-4a2b-8c3d-0123456789ab",
            "event_time": "2019-02-13T14:06:00Z",
            "card_token": "a" * 64,
            "user_id": "19",
            "amount": 80.0,
            "currency": "USD",
            "channel": "chip",
            "merchant_name": "-4282466774399734331",
            "merchant_city": "Tucson",
            "merchant_state": "AZ",
            "merchant_country": "US",
            "zip": "85719",
            "mcc": 4829,
            "errors": None,
            "is_fraud": False,
        }

    def test_sample_event_round_trips_with_nullable_fields_set(self, avro_schema):
        parsed = fastavro.parse_schema(avro_schema)
        event = self._sample_event()

        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, parsed, event)
        buf.seek(0)
        decoded = fastavro.schemaless_reader(buf, parsed)
        assert decoded == event

    def test_sample_event_round_trips_with_nullable_fields_null(self, avro_schema):
        parsed = fastavro.parse_schema(avro_schema)
        event = self._sample_event()
        event["zip"] = None
        event["errors"] = None
        event["is_fraud"] = None

        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, parsed, event)
        buf.seek(0)
        decoded = fastavro.schemaless_reader(buf, parsed)
        assert decoded == event

    def test_generated_avsc_file_is_valid_json_and_parses(self):
        with open(COMMITTED_AVSC_PATH, encoding="utf-8") as f:
            text = f.read()
        parsed = json.loads(text)
        fastavro.parse_schema(parsed)
