# Datos del proyecto

Los datos no se versionan en GitHub por su tamaño. Descargar la carpeta completa de Drive:

**Enlace:** https://drive.google.com/drive/folders/1Hh9hd5r0MctmWPgkJFVBsrwIxs5ECerI?usp=sharing

## Cómo usarla

**En Google Colab (recomendado)**
1. Abrir el enlace y copiar la carpeta `etl_proyecto` a tu Drive, dentro de `Mi unidad/Colab Notebooks/`.
2. Ejecutar los notebooks en orden: 01, 02, 03, merge y, al final, Streamlit.
   Los notebooks montan Drive y leen de esa ruta sin pedir credenciales.

**Fuera de Colab**
1. Descargar la carpeta y dejarla en la raíz del repositorio como `./etl_proyecto`
   (o definir `ETL_BASE` con otra ruta, ver `.env.example`).

## Contenido

| Archivo | Qué es |
|---|---|
| `viviendas_pdtos.csv` | Fuente 2: viviendas (DANE) |
| `db_export/*.csv` | Fuente 3: copia de las tablas de la primera entrega (`fact_personas`, `fact_hogares`, `dim_persona`, `dim_geografia`) |
| `merge_departamento.csv` | Resultado final del pipeline, una fila por departamento |

La fuente 1 (IPM) no se descarga: se consulta por HTTP a la API del proyecto.

Si se define `DATABASE_URL` en los Secretos de Colab, el notebook 03 lee directo de la
base de datos y no necesita `db_export`.
