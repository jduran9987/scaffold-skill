from pathlib import Path

import pytest

from sp_dynamo_firehose_s3 import router

FIXTURES = Path(__file__).parent / "v1" / "fixtures"


def test_routes_to_the_version_in_the_yaml(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setitem(router.VERSIONS, "v1", lambda argv: calls.append(argv) or 7)
    argv = ["scaffold", str(FIXTURES / "orders.yaml"), "--force"]

    assert router.main(argv) == 7
    assert calls == [argv]


@pytest.mark.parametrize("version", ["v0", "1", "null", "v2"])
def test_rejects_unsupported_versions(tmp_path, capsys, version):
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(f"version: {version}\n")

    assert router.main(["scaffold", str(config_path)]) == 1
    assert "Supported: v1" in capsys.readouterr().err


def test_reports_missing_file(tmp_path, capsys):
    assert router.main(["scaffold", str(tmp_path / "missing.yaml")]) == 1
    assert "missing.yaml" in capsys.readouterr().err


def test_reports_non_mapping_yaml(tmp_path, capsys):
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text("- not\n- a mapping\n")

    assert router.main(["scaffold", str(config_path)]) == 1
    assert "Expected a YAML mapping" in capsys.readouterr().err


def test_reports_invalid_yaml(tmp_path, capsys):
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text("version: [v1\n")

    assert router.main(["scaffold", str(config_path)]) == 1
    assert "Error:" in capsys.readouterr().err


@pytest.mark.parametrize("argv", [[], ["scaffold"], ["--help"], ["scaffold", "-h"]])
def test_arguments_without_a_yaml_path_go_to_the_latest_version(argv, monkeypatch):
    calls = []
    monkeypatch.setitem(router.VERSIONS, "v1", lambda args: calls.append(args) or 0)

    assert router.main(argv) == 0
    assert calls == [argv]


def test_usage_errors_come_from_the_latest_version(capsys):
    with pytest.raises(SystemExit) as exit_info:
        router.main([])

    assert exit_info.value.code == 2
    assert "action" in capsys.readouterr().err


def test_yaml_that_does_not_exist_goes_to_the_latest_version(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setitem(router.VERSIONS, "v1", lambda args: calls.append(args) or 0)
    argv = ["scaffold", str(tmp_path / "new.yaml")]

    assert router.main(argv) == 0
    assert calls == [argv]


def test_generate_yaml_runs_end_to_end(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert router.main(["generate-yaml", "orders"]) == 0
    text = (tmp_path / "pipelines" / "orders.yaml").read_text()
    assert text.startswith("version: v1\n")


def test_table_names_go_to_the_latest_version(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setitem(router.VERSIONS, "v1", lambda args: calls.append(args) or 0)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "orders").write_text("not yaml: [")

    assert router.main(["create-stream", "orders"]) == 0
    assert calls == [["create-stream", "orders"]]
