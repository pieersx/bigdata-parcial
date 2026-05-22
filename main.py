import json

from app.pipelines.bronze.bronze_pipeline import BronzePipeline


def main():
    pipeline = BronzePipeline()
    result = pipeline.run()

    print("Partial Bronze Pipeline Report")
    print(f"{'=' * 70}")
    print(f"Execution ID: {result['execution_id']}")
    print(f"Total Assets: {result['total_assets']}")
    print(f"Successful: {result['successful']}")
    print(f"Skipped Existing: {result['skipped_existing']}")
    print(f"Failed: {result['failed']}")
    print("\nExecution Report:")
    print(json.dumps(result['execution_report'], indent=2, default=str))


if __name__ == "__main__":
    main()
