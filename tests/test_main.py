from main import main


def test_main_runs_only_enabled_datasets(monkeypatch):

    called_datasets = []

    def fake_run_pipeline(dataset_name, config):
        called_datasets.append(dataset_name)

    monkeypatch.setattr(
        "main.run_pipeline",
        fake_run_pipeline,
    )

    monkeypatch.setattr(
        "main.SparkSessionManager.get_session",
        lambda: None,
    )

    monkeypatch.setattr(
        "main.SparkSessionManager.stop_session",
        lambda: None,
    )

    main()

    assert called_datasets == ["customers"]