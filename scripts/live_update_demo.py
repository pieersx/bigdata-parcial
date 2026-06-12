import json
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.clients.download_client import DownloadClient
from app.config.settings import settings


LIVE_YEARS = {2025, 2026}


def main() -> int:
    print("Live update demo: SIAF 2025-2026 -> Bronze -> Silver -> Gold -> Power BI workbook")
    print("=" * 80)
    _print_live_sources()

    commands = [
        [sys.executable, "main.py", "ingresos", "categorias_municipalidades"],
        [sys.executable, "main_silver.py"],
        [sys.executable, "main_gold.py"],
        [sys.executable, "scripts/export_powerbi_workbook.py"],
    ]
    for command in commands:
        print(f"\n$ {' '.join(command)}")
        subprocess.run(command, check=True)

    print("\nLive update demo completed.")
    return 0


def _print_live_sources() -> None:
    client = DownloadClient(timeout=settings.timeout, max_retries=settings.max_retries)
    ingresos = settings.datasets["ingresos"]
    for resource in ingresos.get("historico", []):
        if int(resource["anio"]) not in LIVE_YEARS:
            continue
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
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    raise SystemExit(main())
