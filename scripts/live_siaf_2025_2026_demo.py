import json
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.clients.download_client import DownloadClient
from app.config.settings import settings
from app.pipelines.bronze.bronze_pipeline import BronzePipeline


LIVE_YEARS = {2025, 2026}


def main() -> int:
    print("Demo carga viva: SIAF ingresos 2025-2026 -> Bronze -> Silver -> Gold -> Excel Power BI")
    print("=" * 90)
    _print_live_sources()

    bronze_result = _run_bronze_live_years_only()
    print("\nBronze live SIAF result:")
    print(json.dumps(_compact_bronze_result(bronze_result), indent=2, ensure_ascii=False, default=str))

    commands = [
        [sys.executable, "main_silver.py"],
        [sys.executable, "main_gold.py"],
        [sys.executable, "scripts/export_powerbi_workbook.py"],
    ]
    for command in commands:
        print(f"\n$ {' '.join(command)}")
        subprocess.run(command, check=True)

    print("\nDemo completed.")
    print("Bronze solo refresco las particiones 2025 y 2026 de SIAF ingresos.")
    print("Silver, Gold y el Excel quedaron recalculados sobre el lake completo.")
    return 0


def _print_live_sources() -> None:
    client = DownloadClient(timeout=settings.timeout, max_retries=settings.max_retries)
    for resource in _live_resources():
        raw_path = settings.raw_path / "ingresos" / resource["filename"]
        remote_metadata = client.remote_metadata(resource["url"])
        local_metadata = client.local_metadata(raw_path)
        payload = {
            "year": resource["anio"],
            "url": resource["url"],
            "raw_path": str(raw_path),
            "local_exists": raw_path.exists(),
            "local_size": raw_path.stat().st_size if raw_path.exists() else None,
            "remote": remote_metadata,
            "local_metadata": local_metadata,
            "decision": "download_if_size_or_last_modified_changed",
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))


def _run_bronze_live_years_only() -> dict:
    pipeline = BronzePipeline()
    try:
        ingresos_pipeline = pipeline.dataset_pipelines["ingresos"]
        ingresos_pipeline.dataset_config = {
            **ingresos_pipeline.dataset_config,
            "historico": _live_resources(),
            "api": [],
        }
        return pipeline.run(dataset_names=["ingresos"])
    finally:
        pipeline.stop()


def _live_resources() -> list[dict]:
    return [
        resource
        for resource in settings.datasets["ingresos"].get("historico", [])
        if int(resource["anio"]) in LIVE_YEARS
    ]


def _compact_bronze_result(result: dict) -> dict:
    return {
        "execution_id": result.get("execution_id"),
        "total_assets": result.get("total_assets"),
        "successful": result.get("successful"),
        "skipped_existing": result.get("skipped_existing"),
        "failed": result.get("failed"),
        "failed_quality_checks": result.get("failed_quality_checks"),
        "assets": [
            {
                "asset_name": detail.get("asset_name"),
                "status": detail.get("status"),
                "records_count": detail.get("records_count"),
                "raw_path": detail.get("raw_path"),
                "bronze_path": detail.get("bronze_path"),
            }
            for detail in result.get("details", [])
        ],
    }


if __name__ == "__main__":
    raise SystemExit(main())
