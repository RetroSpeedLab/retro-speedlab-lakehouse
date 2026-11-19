from lakehouse import mining, nightly


def test_the_night_measures_every_table_once_in_the_nightly_run() -> None:
    assert "platform" in nightly.task_ids
    assert "platform" not in mining.task_ids


def test_the_nightly_chain_gets_the_slot_before_mining_and_the_jobs_aside() -> None:
    experiments = nightly.get_task("experiments").priority_weight
    mined = mining.get_task("mine-records").priority_weight
    assert experiments > mined > nightly.get_task("maintenance").priority_weight
