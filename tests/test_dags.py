from lakehouse import mining, nightly


def test_the_night_measures_every_table_once_in_the_nightly_run() -> None:
    assert "platform" in nightly.task_ids
    assert "platform" not in mining.task_ids
