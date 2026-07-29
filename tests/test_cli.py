from click.testing import CliRunner

from gundi_dlq import main


def test_help():
    result = CliRunner().invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "--from-sub" in result.output


def test_reprocess_and_purge_are_mutually_exclusive():
    result = CliRunner().invoke(
        main, ["--from-sub", "some-sub", "--reprocess", "--purge"]
    )
    assert result.exit_code == 1
    assert "Cannot use --reprocess and --purge together" in result.output


def test_reprocess_or_purge_is_required():
    result = CliRunner().invoke(main, ["--from-sub", "some-sub"])
    assert result.exit_code == 1
    assert "Must use either --reprocess or --purge" in result.output


def test_reprocess_requires_target_topic():
    result = CliRunner().invoke(main, ["--from-sub", "some-sub", "--reprocess"])
    assert result.exit_code == 1
    assert "Must provide a target topic with --reprocess" in result.output


def test_from_sub_is_required():
    result = CliRunner().invoke(main, ["--reprocess", "--to-topic", "some-topic"])
    assert result.exit_code == 2
    assert "--from-sub" in result.output
