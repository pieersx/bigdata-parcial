# Verificacion de datasets oficiales vs archivos locales

Fecha de revision: 2026-06-05

Fuentes revisadas:

- SIAF Ingresos MEF: https://datosabiertos.mef.gob.pe/dataset/presupuesto-y-ejecucion-de-ingreso
- SISMEPRE MEF: https://datosabiertos.mef.gob.pe/dataset/seguimiento-de-la-meta-del-impuesto-predial
- RENAMU 2022 INEI: https://www.datosabiertos.gob.pe/dataset/registro-nacional-de-municipalidades-renamu-2022-instituto-nacional-de-estad%C3%ADstica-e

## Resultado Ejecutivo

No todos los archivos locales son exactamente iguales a los recursos oficiales actuales.

- SIAF Ingresos 2012-2024 coincide con los archivos oficiales por tamano exacto.
- SIAF Ingresos 2025 y 2026 no coincide por tamano contra el archivo oficial actual.
- SISMEPRE tiene tres tablas locales desactualizadas frente al total oficial actual.
- RENAMU ZIP y PDF coinciden con el portal oficial, pero Bronze conserva menos filas que el CSV extraido.

## SIAF Ingresos

El `config.yaml` apunta a los archivos oficiales correctos de MEF bajo `fs.datosabiertos.mef.gob.pe`.

| Archivo | Filas locales | Bytes locales | Bytes oficiales | Estado |
|---|---:|---:|---:|---|
| `2012-Ingreso.csv` | 759,578 | 367,063,258 | 367,063,258 | OK |
| `2013-Ingreso.csv` | 721,818 | 348,780,839 | 348,780,839 | OK |
| `2014-Ingreso.csv` | 754,093 | 364,949,661 | 364,949,661 | OK |
| `2015-Ingreso.csv` | 781,007 | 380,808,487 | 380,808,487 | OK |
| `2016-Ingreso.csv` | 764,729 | 372,391,131 | 372,391,131 | OK |
| `2017-Ingreso.csv` | 683,816 | 332,850,769 | 332,850,769 | OK |
| `2018-Ingreso.csv` | 656,859 | 319,162,479 | 319,162,479 | OK |
| `2019-Ingreso.csv` | 706,695 | 343,748,500 | 343,748,500 | OK |
| `2020-Ingreso.csv` | 692,729 | 338,329,940 | 338,329,940 | OK |
| `2021-Ingreso.csv` | 828,167 | 403,458,098 | 403,458,098 | OK |
| `2022-Ingreso.csv` | 774,954 | 377,350,028 | 377,350,028 | OK |
| `2023-Ingreso.csv` | 743,452 | 362,173,711 | 362,173,711 | OK |
| `2024-Ingreso.csv` | 758,116 | 369,942,256 | 369,942,256 | OK |
| `2025-Ingreso-Mensual.csv` | 878,729 | 429,297,253 | 429,296,872 | Revisar |
| `2026-Ingreso-Mensual.csv` | 272,326 | 131,964,621 | 172,232,062 | Desactualizado |

Bronze SIAF conserva las mismas filas que los CSV locales actuales por anio:

- Total local SIAF, sin diccionario: `10,777,068` filas.
- Total Bronze SIAF, sin diccionario: `10,777,068` filas.

Conclusion SIAF: el pipeline no esta perdiendo filas locales, pero 2025 y 2026 no son identicos al archivo oficial actual.

## SISMEPRE

El `config.yaml` apunta a los `resource_id` oficiales usados por la API MEF. La comparacion se hizo contra `result.include_total`.

| Archivo | Filas locales | Total oficial API | Estado |
|---|---:|---:|---|
| `rentas_preguntas.csv` | 696 | 836 | Desactualizado |
| `rentas_estadistica.csv` | 233 | 233 | OK |
| `rentas_formulario.csv` | 94 | 98 | Desactualizado |
| `rentas_esat_estadistica_atm.csv` | 133,172 | 133,810 | Desactualizado |
| `rentas_respuestas.csv` | 174,210 | 174,210 | OK |
| `rentas_ano_aplicacion.csv` | 26 | 26 | OK |
| `rentas_entidad_estado.csv` | 19,037 | 19,037 | OK |

Bronze SISMEPRE conserva las mismas filas que los CSV locales, por eso el problema no esta en Bronze sino en que algunos CSV locales quedaron antiguos.

## RENAMU 2022

El portal oficial RENAMU enlaza al mismo diccionario PDF y al mismo ZIP usados en `config.yaml`.

| Archivo | Bytes locales | Bytes oficiales | Estado |
|---|---:|---:|---|
| `renamu_diccionario_2022.pdf` | 718,281 | 718,281 | OK |
| `renamu_2022.zip` | 1,919,681 | 1,919,681 | OK |

Pero el CSV extraido y Bronze no tienen el mismo conteo:

| Tabla | Filas |
|---|---:|
| `data/raw/renamu/Base_RENAMU_2022_f.csv` | 1,874 |
| `data/bronze/renamu` | 1,150 |

Conclusion RENAMU: la fuente oficial local es correcta, pero la lectura Spark actual de RENAMU esta perdiendo filas. La causa probable es lectura CSV sin `multiLine=true` en un archivo ancho con campos complejos.

## Acciones Recomendadas

1. Reingestar SIAF 2025 y 2026 desde las URLs actuales.
2. Reingestar SISMEPRE permitiendo refresco cuando el total oficial API sea distinto al CSV local.
3. Leer RENAMU con `multiLine: true` y reconstruir Bronze/Silver/Gold.
4. Reejecutar Bronze, Silver, Gold y regenerar Power BI despues de actualizar las fuentes.

