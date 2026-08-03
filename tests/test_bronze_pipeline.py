from pipeline.bronze.bronze_pipeline import BronzePipeline

def test_bronze_pipeline_creation():
    pipeline = BronzePipeline()

    assert pipeline.reader is not None
    assert pipeline.writer is not None