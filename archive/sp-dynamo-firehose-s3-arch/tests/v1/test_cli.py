from pathlib import Path

import pytest

from sp_dynamo_firehose_s3.v1 import main

FIXTURES = Path(__file__).parent / "fixtures"


def test_scaffold_generates_files_from_yaml(tmp_path, capsys):
    exit_code = main(
        ["scaffold", str(FIXTURES / "orders.yaml"), "--output-dir", str(tmp_path)]
    )

    assert exit_code == 0
    bronze_path = tmp_path / "src" / "orders" / "bronze.py"
    assert bronze_path.exists()
    assert f"Generated {bronze_path}" in capsys.readouterr().out


def test_scaffold_reports_validation_errors(tmp_path, capsys):
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text("version: v1\n")

    assert (
        main(["scaffold", str(config_path), "--output-dir", str(tmp_path / "out")]) == 1
    )
    assert "metadata" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()


def test_scaffold_rejects_other_versions(tmp_path, capsys):
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text("version: v2\n")

    assert main(["scaffold", str(config_path)]) == 1
    assert "version" in capsys.readouterr().err


def test_scaffold_reports_existing_files(tmp_path, capsys):
    argv = ["scaffold", str(FIXTURES / "orders.yaml"), "--output-dir", str(tmp_path)]
    assert main(argv) == 0

    assert main(argv) == 1
    assert "Output files already exist" in capsys.readouterr().err


def test_requires_an_action(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main([])

    assert exit_info.value.code == 2
    assert "action" in capsys.readouterr().err


def test_rejects_unknown_actions(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["deploy", "orders.yaml"])

    assert exit_info.value.code == 2
    assert "invalid choice: 'deploy'" in capsys.readouterr().err


def test_scaffold_requires_a_config_path(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["scaffold"])

    assert exit_info.value.code == 2
    assert "config_path" in capsys.readouterr().err
