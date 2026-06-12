import json

from app.pipelines.gold.gold_pipeline import GoldPipeline


def main():
    pipeline = GoldPipeline()
    try:
        result = pipeline.run()
        print("Gold Municipal Analytics Pipeline Report")
        print("=" * 70)
        print(f"Execution ID: {result['execution_id']}")
        print(f"Status: {result['status']}")
        print(f"Published Tables: {result['published_tables']}")
        print(f"Records Published: {result['records_published']}")
        print(f"Failed Quality Checks: {result['failed_quality_checks']}")
        print("\nErrors:")
        print(json.dumps(result["errors"], indent=2, default=str))
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
