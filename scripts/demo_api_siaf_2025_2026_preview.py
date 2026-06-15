from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.clients.download_client import DownloadClient
from app.config.settings import settings


LIVE_YEARS = {2025, 2026}


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Demo segura de carga viva SIAF 2025-2026. "
            "No descarga, no escribe raw, no ejecuta pipelines."
        )
    )
    parser.add_argument(
        "--output",
        default="data/audit/metrics/live_siaf_2025_2026_preview.json",
        help="Ruta donde guardar la evidencia JSON de la inspeccion remota.",
    )
    args = parser.parse_args()

    client = DownloadClient(timeout=settings.timeout, max_retries=settings.max_retries)
    resources = [
        resource
        for resource in settings.datasets["ingresos"].get("historico", [])
        if int(resource["anio"]) in LIVE_YEARS
    ]

    preview = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "mode": "preview_only_no_write_no_pipeline",
        "purpose": "Demostracion segura de consulta remota SIAF 2025-2026 para exposicion.",
        "would_run_if_confirmed": [
            "python main.py ingresos",
            "python main_silver.py",
            "python main_gold.py",
            "python scripts/export_powerbi_workbook.py",
        ],
        "resources": [],
    }

    for resource in resources:
        raw_path = settings.raw_path / "ingresos" / resource["filename"]
        remote_metadata = client.remote_metadata(resource["url"])
        local_metadata = client.local_metadata(raw_path)
        preview["resources"].append(
            {
                "year": int(resource["anio"]),
                "source_url": resource["url"],
                "configured_raw_path": str(raw_path),
                "local_exists": raw_path.exists(),
                "local_size": raw_path.stat().st_size if raw_path.exists() else None,
                "remote_metadata": remote_metadata,
                "local_metadata": local_metadata,
                "safe_decision": "solo_mostrar_metadata_no_descargar",
            }
        )

    output_path = PROJECT_ROOT / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(preview, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(preview, indent=2, ensure_ascii=False))
    print(f"\nEvidencia guardada en: {output_path}")
    print("No se modifico Bronze, Silver, Gold ni Power BI.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
