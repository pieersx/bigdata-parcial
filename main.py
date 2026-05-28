import argparse
import json

from app.pipelines.bronze.bronze_pipeline import BronzePipeline


def parse_args():
    parser = argparse.ArgumentParser(description="Execute Bronze parquet pipeline")
    parser.add_argument(
        "datasets",
        nargs="*",
        default=None,
        help="Datasets to process: ingresos sismepre renamu",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    pipeline = BronzePipeline()
    try:
        result = pipeline.run(dataset_names=args.datasets or None)

        print("Bronze Parquet Pipeline Report")
        print(f"{'=' * 70}")
        print(f"Execution ID: {result['execution_id']}")
        print(f"Total Assets: {result['total_assets']}")
        print(f"Successful: {result['successful']}")
        print(f"Skipped Existing: {result['skipped_existing']}")
        print(f"Skipped Optional: {result['skipped_optional']}")
        print(f"Failed: {result['failed']}")
        print("\nExecution Report:")
        print(json.dumps(result['execution_report'], indent=2, default=str))
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
