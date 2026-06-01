import json

from app.pipelines.silver.silver_pipeline import SilverPipeline


def main():
    pipeline = SilverPipeline()
    try:
        result = pipeline.run()
        print("Silver Municipal Pipeline Report")
        print("=" * 70)
        print(f"Execution ID: {result['execution_id']}")
        print(f"Status: {result['status']}")
        print(f"Published Tables: {result['published_tables']}")
        print(f"Blocked Tables: {result['blocked_tables']}")
        print(f"Records Published: {result['records_published']}")
        print(f"Records Quarantined: {result['records_quarantined']}")
        print(f"Failed Quality Checks: {result['failed_quality_checks']}")
        print("\nBlocked:")
        print(json.dumps(result["blocked"], indent=2, default=str))
        print("\nErrors:")
        print(json.dumps(result["errors"], indent=2, default=str))
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
