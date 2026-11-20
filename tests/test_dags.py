from lakehouse import hourly, mining, nightly


def test_the_night_measures_every_table_once_in_the_nightly_run() -> None:
    assert "platform" in nightly.task_ids
    assert "platform" not in mining.task_ids


def test_the_jobs_aside_wait_for_the_whole_chain() -> None:
    for dag, last, aside in ((nightly, "platform", "maintenance"), (hourly, "gold", "sources-hourly")):
        assert dag.get_task(aside).upstream_task_ids == {last}


def test_the_nightly_chain_gets_the_slot_before_mining_and_the_jobs_aside() -> None:
    experiments = nightly.get_task("experiments").priority_weight
    mined = mining.get_task("mine-records").priority_weight
    assert experiments > mined > nightly.get_task("maintenance").priority_weight
