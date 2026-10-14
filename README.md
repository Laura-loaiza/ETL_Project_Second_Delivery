# Pobreza, Pobreza Extrema e Indigencia en Colombia — Proyecto ETL (G51)

**Universidad Autónoma de Occidente** — Facultad de Ingeniería y Ciencias Básicas
**Programa:** Ingeniería de Datos e Inteligencia Artificial · **Curso:** ETL (G51)

**Integrantes**
- Sebastián Alejandro Jiménez Solís
- Laura Valentina Loaiza Díaz
- Santiago Lourido Rodríguez
- Carlos Fabián Córdoba

---

## 1. Visión general y objetivos

Analizar la pobreza monetaria, la pobreza extrema (indigencia) y la pobreza multidimensional (IPM) en Colombia, y qué condiciones de los hogares y de las personas la acompañan, a partir de datos oficiales del DANE y de una API propia.

**Objetivo de la Entrega 1.** Construir un ETL sobre los microdatos de la GEIH (Hogares y Personas), modelarlos en un esquema de galaxia en PostgreSQL (Supabase) y responder preguntas de negocio con consultas SQL ponderadas por factor de expansión (ODS 1: Fin de la pobreza).

**Objetivo refinado de la Entrega 2.** Automatizar la ingesta de tres fuentes y compararlas entre sí. La pregunta central es: *¿coincide el mapa de pobreza monetaria (GEIH) con el de pobreza multidimensional (IPM)? ¿Qué departamentos se ven distintos según la medida y qué condiciones de vivienda los acompañan?* Para responderla, cada fuente cumple un propósito:

| Fuente | Qué aporta al objetivo |
|---|---|
| **API (IPM)** | Pobreza multidimensional por hogar: 15 indicadores de privación, índice `ipm`, marca `pobre` y factores de expansión. |
| **CSV (viviendas)** | Condiciones de la vivienda que están detrás de cuatro indicadores del IPM: material de paredes y pisos, alcantarillado, acueducto y clase (cabecera, centro poblado, rural disperso). |
| **Base de datos (Entrega 1)** | Pobreza monetaria, ingresos, empleo y educación de las personas (GEIH), para comparar por territorio. |

**Resultados que debe entregar el pipeline (Entrega 2).**
1. Un ranking de departamentos según pobreza monetaria (GEIH) y según pobreza multidimensional (IPM), y qué tan distintos son (departamentos que "suben" o "bajan" según la medida).
2. Qué privaciones de vivienda (paredes, pisos, alcantarillado, acueducto) acompañan a los departamentos donde las dos medidas difieren.
3. Diferencias urbano / rural (`clase`) en la pobreza multidimensional y en las condiciones de la vivienda.
4. Un dataset final enriquecido, validado y cargado en el data warehouse, con visualizaciones que salen **solo** de ese warehouse.

### Preguntas de negocio de la Entrega 1
Nivel hogar:
1. ¿Los hogares con mayor número de personas presentan mayor probabilidad de estar en pobreza extrema?
2. ¿En qué departamentos se concentran los hogares con menores ingresos?
3. ¿La pobreza extrema se distribuye igual entre zonas urbanas y rurales, y qué tan lejos están de salir de ella los hogares en indigencia?

Nivel persona:
4. ¿Las personas de hogares pobres tienen mayor presencia en trabajos de baja remuneración o en múltiples empleos?
5. ¿Dónde hay más pobreza extrema a nivel de personas?
6. ¿Quiénes están en pobreza extrema (sexo, edad)?
7. ¿Qué relación tiene el nivel educativo con la pobreza extrema?
8. ¿Cómo se comportan los ingresos de las personas según la condición de pobreza de su hogar?

---

## 2. Estado de la Entrega 2

| Etapa | Estado |
|---|---|
| Extracción, EDA, transformación y validación con Great Expectations de las 3 fuentes | Listo (notebooks 01, 02 y 03) |
| Pobreza monetaria agregada por departamento (GEIH, ponderada con `fex_c`) | Listo en el notebook 03 (`db_monetaria_dpto.parquet`); falta su suite de validación |
| Merge de las fuentes | Pendiente |
| Validación con Great Expectations sobre el dataset unido | Pendiente |
| Modelo dimensional y carga al data warehouse | Pendiente |
| DAG de Apache Airflow | Pendiente |
| Visualizaciones y dashboard | Pendiente |

Este README documenta la parte lista (extracción de las tres fuentes) y deja marcado lo que falta. La sección 12 relaciona cada punto del entregable con su estado.

---

## 3. Stack tecnológico

| Capa | Tecnología | Para qué se usa |
|---|---|---|
| Entorno de desarrollo | Google Colab + Google Drive | Ejecución en la nube, trabajo en equipo y archivos compartidos. |
| Procesamiento | Python (pandas, numpy, pyarrow, scipy) | Lectura, limpieza, transformación y archivos Parquet. |
| Ingesta de la API | requests + urllib3 | Peticiones con reintentos y espera creciente. |
| Base de datos | PostgreSQL (Supabase) | Contiene el modelo de la Entrega 1 (fuente 3) y será el warehouse. |
| Conexión Python ↔ BD | SQLAlchemy + psycopg2-binary | Lectura de tablas por bloques. |
| Calidad de datos | Great Expectations 1.x (`>=1.0,<2`) | Suites de validación, con registro de cada regla. |
| Gráficos | matplotlib | Gráficos del EDA. |
| Orquestación | Apache Airflow | Pendiente: DAG con las tres ramas. |
| Control de versiones | Git / GitHub | Código y documentación. |

---

## 4. Arquitectura y flujo

```
  API Docker (IPM)          CSV (viviendas_pdtos.csv)      Supabase (modelo Entrega 1)
        │                            │                              │
   read_api                     read_csv                        read_db
        │                            │                              │
   transform_api               transform_csv                  transform_db
        │                            │                       + resumen_hogar (solo EDA)
        │                            │                       + resumen_monetaria (por dpto)
   validar (GE)                 validar (GE)                   validar (GE)
        │                            │                              │
 ipm_api_clean.parquet       csv_clean.parquet         db_personas_clean.parquet
                                                       db_hogares_clean.parquet
                                                       db_monetaria_dpto.parquet
        └───────────────┬────────────┘                              │
                        ▼                                           ▼
              MERGE por hogar (id_hogar)            AGREGADOS por departamento  [pendiente]
                        └─────────────────────┬─────────────────────┘
                                              ▼
                    Validación GE del dataset unido  ──(si falla, no se carga)  [pendiente]
                                              ▼
                              Carga al Data Warehouse (PostgreSQL)  [pendiente]
                                              ▼
                               Visualización / dashboard  [pendiente]

      Orquestación: Apache Airflow (una tarea por rama, luego merge → validación → carga)  [pendiente]
```

---

## 5. Extracción de las tres fuentes

Los tres notebooks siguen la misma estructura: **leer → EDA sobre datos crudos → transformar → EDA sobre datos transformados → hallazgos → validar con Great Expectations → guardar solo si la validación pasa.** Las funciones principales (`read_*`, `transform_*`, `reglas_*` y `validar`) están escritas para pasarse después a tareas del DAG de Airflow.

### 5.1 Resumen

| | Fuente 1 · API | Fuente 2 · CSV | Fuente 3 · Base de datos |
|---|---|---|---|
| Notebook | `01_fuente_api_ipm_def.ipynb` | `02_fuente_csv_def.ipynb` | `03_fuente_base_de_datos_def.ipynb` |
| Origen | API en un contenedor Docker propio | `viviendas_pdtos.csv` (DANE, IPM 2025, "Viviendas (Departamental)") | Supabase / PostgreSQL (modelo de la Entrega 1, GEIH) |
| Grano | Hogar | Hogar | Persona (se resume a hogar) |
| Llave | `directorio + secuencia_p` → `id_hogar` | `directorio + secuencia_p` → `id_hogar` | `directorio + secuencia_p` → `id_hogar` |
| Tamaño de la última corrida | ≈ 87.060 hogares tras la limpieza _(confirmar columnas y duplicados con la corrida)_ | 87.060 filas crudas → 86.848 sin duplicados | 773.932 personas · 276.666 hogares |
| Salida limpia | `ipm_api_clean.parquet` | `csv_clean.parquet` | `db_personas_clean.parquet`, `db_hogares_clean.parquet` y `db_monetaria_dpto.parquet` |
| Suite de validación | `suite_api_ipm` | `suite_csv` | `suite_db_personas` y `suite_db_hogares` |

### 5.2 Fuente 1 — API del IPM (`01_fuente_api_ipm_def.ipynb`)

**Qué es.** La API entrega el archivo de hogares del IPM: una fila por hogar con los 15 indicadores de privación (0 o 1), el `ipm`, la marca `pobre`, `personas` y los factores de expansión `fex_c` (hogares) y `fexp` (personas).

**Por qué esta API.**
- Los datos son estructurados, oficiales (DANE) y corresponden directamente al problema: pobreza multidimensional.
- Al servirla desde un contenedor propio el equipo controla la disponibilidad y el formato, y la extracción queda reproducible y lista para programarse en Airflow.
- Limitaciones, que se asumen y se documentan: usa HTTP sin autenticación, los datos son una foto del archivo del DANE (no cambian entre corridas, salvo que se actualice el contenedor) y la dirección es una IP fija: si el servidor está apagado, el notebook no puede extraer. El nombre del endpoint (`/productos`) no refleja su contenido.

**Frecuencia de extracción.** Como el contenido es estable, ejecutar la tarea a diario no trae datos nuevos, pero sí comprueba que el servicio sigue disponible y que el archivo no cambió. Diseño previsto para la tarea en Airflow: en cada corrida comparar número de filas y una huella (hash) del contenido con la corrida anterior, registrar si hubo cambios y mantener la carga idempotente (recarga completa, sin duplicar).

**Configuración.** La dirección se busca en este orden: variable de entorno `API_URL` → secretos de Colab → valor por defecto `http://201.32.130.152/api/productos`. Timeout de 120 s.

**Extracción (`read_api`).** Sesión HTTP con 4 reintentos y espera creciente (`backoff_factor=2`) ante errores 500, 502, 503 y 504. Acepta que la respuesta sea una lista o un diccionario con la lista dentro, y sigue la paginación si la API la trae. Guarda el crudo en `ipm_api_raw.csv`.

**EDA sobre datos crudos.** Tipos, nulos (incluido el texto `"NA"`), duplicados exactos y por llave, estadísticos y atípicos por rango intercuartílico, y una **comprobación de consistencia interna** de la lógica del IPM:
- `ipm = Σ (indicador × peso)`, con los pesos de `PESOS_IPM` (5 dimensiones de 20 % cada una: educación, niñez y juventud, trabajo, salud, vivienda y servicios).
- `pobre = 1` si y solo si `ipm ≥ 1/3`.
- `fexp = fex_c × personas`.

**Transformación (`transform_api`).**
- Nombres de columnas en minúscula y `"NA"` convertido a nulo.
- `departamento` (entero) pasa a `codigo_departamento` de 2 dígitos y se agrega `nombre_departamento` (catálogo `DEPARTAMENTOS`).
- Se crea `id_hogar = directorio-secuencia_p`.
- `periodo` solo viene lleno en parte de las filas: se convierte a fecha y se crea `fecha_corte` con la fecha más frecuente.
- Se agrega `n_privaciones` (cuántos de los 15 indicadores valen 1).
- Se descartan las columnas casi vacías (`COLS_DESCARTAR`) y `secuencia_encuesta` si es idéntica a `personas`.
- Se eliminan filas completamente duplicadas.

**Resumen por departamento.** Se calcula el IPM promedio ponderado por departamento: `ipm_ponderado = Σ(ipm × fex_c) / Σ(fex_c)`, en escala 0 a 1. (Un promedio de `ipm × fex_c` sin dividir por la suma de pesos no sirve: mezcla el nivel de pobreza con el tamaño de los factores de expansión.)

**EDA visual.** Porcentaje de hogares pobres (`fex_c`) y de personas pobres (`fexp`), distribución del `ipm`, número de privaciones, tasa de privación por indicador y pobreza por departamento. Los departamentos con menos de 30 hogares en la muestra se marcan como poco confiables.

**Validación (`suite_api_ipm`).** Llaves e indicadores sin nulos; indicadores y `pobre` en {0, 1}; `id_hogar` único; `ipm` entre 0 y 1; `personas` entre 1 y 30; `n_privaciones` entre 0 y 15; factores de expansión positivos; `codigo_departamento` dentro del catálogo; y las tres comprobaciones de consistencia: `pobre` debe coincidir en todas las filas; `ipm` (diferencia absoluta menor a 0,005 frente a la suma ponderada) y `fexp` (igual a `fex_c × personas` con tolerancia relativa de 0,01 %) se exigen en al menos el 99 % de las filas (`mostly=0.99`).

### 5.3 Fuente 2 — CSV de viviendas (`02_fuente_csv_def.ipynb`)

**Qué es.** El archivo "Viviendas (Departamental)" del IPM 2025 del DANE ([diccionario](https://microdatos.dane.gov.co/index.php/catalog/903/data-dictionary/F6?file_name=Viviendas%20(Departamental))). Según el diccionario tiene 8 variables y 86.848 casos; el archivo que usamos trae además una columna `DEPARTAMENTO` (9 columnas) y 87.060 filas, de las cuales 212 son duplicados exactos.

**Por qué esta fuente.** Son las respuestas crudas detrás de cuatro indicadores del IPM (paredes, pisos, alcantarillado y acueducto). Aportan el *tipo* de material o servicio y la clase (cabecera, centro poblado, rural disperso), y permiten comprobar que las respuestas sean coherentes con los indicadores de la fuente 1.

| Variable original | Columna final | Significado |
|---|---|---|
| `P3` | `clase` | 1 Cabecera, 2 Centro poblado, 3 Rural disperso |
| `P4005` | `material_paredes` | Material predominante de las paredes exteriores (códigos 1 a 9) |
| `P4015` | `material_pisos` | Material predominante de los pisos (códigos 1 a 7) |
| `P8520S3` | `alcantarillado_vivienda` | 1 Sí, 2 No |
| `P8520S5` | `acueducto_vivienda` | 1 Sí, 2 No |

El sufijo `_vivienda` evita chocar con los indicadores `acueducto` y `alcantarillado` de la fuente 1.

**Lectura (`read_csv`).** Ruta por defecto `etl_proyecto/viviendas_pdtos.csv` en Drive; se puede cambiar con la variable de entorno `CSV_PATH`. Si el archivo no está, Colab abre el cuadro para subirlo. Prueba UTF-8 y, si falla, Latin-1. Guarda el crudo en `csv_raw.parquet`.

**EDA.** Tipos, nulos, duplicados, llave, distribución de cada variable y relación entre variables con la V de Cramér.

**Transformación (`transform_csv`).**
- Nombres en minúscula y renombre con `RENOMBRAR`.
- Columnas a numérico (entero con soporte de nulos cuando corresponde); los valores no numéricos pasan a nulo y se avisa cuántos son.
- Se eliminan duplicados exactos (212).
- `codigo_departamento` de 2 dígitos y `nombre_departamento`.
- `id_hogar = directorio-secuencia_p`.
- Por cada variable se agrega una columna `<variable>_desc` con el texto de cada código (`ETIQUETAS`) y se conserva el código original.
- Si quedan hogares con la misma llave pero valores distintos, solo se avisa: no se borran sin decisión del equipo.

**Comparación con la fuente 1.** Cruce por `id_hogar` con `ipm_api_clean.parquet` (si ya existe): hogares en cada fuente, porcentaje de cobertura y si `secuencia_encuesta` coincide con `personas`. Además muestra la pobreza multidimensional según cada variable de vivienda y la relación entre cada respuesta cruda y su indicador del IPM.

**Validación (`suite_csv`).** Llaves sin nulos y `directorio + secuencia_p` única; `secuencia_p` entre 1 y 6; `secuencia_encuesta` entre 1 y 21; cada variable solo con códigos del diccionario (`VALORES_VALIDOS`, tolerando hasta 1 % de códigos sin describir, como el código 10 de `P4005` que aparece en 108 casos, 0,12 %); y variables sin nulos (tolerando 5 %).

### 5.4 Fuente 3 — Base de datos (`03_fuente_base_de_datos_def.ipynb`)

**Qué es.** El modelo de la primera entrega en Supabase: la tabla de hechos `fact_personas` unida a `dim_persona` y a `dim_geografia`. Los datos vienen de la GEIH del DANE.

**Por qué esta fuente.** Aporta la pobreza monetaria (`fact_hogares.pobre`) y las características de las personas (ingreso, empleo, educación, edad), que se comparan con la pobreza multidimensional. Tablas del modelo y su tamaño: `dim_geografia` 76, `dim_tiempo` 12, `dim_hogar` y `fact_hogares` 276.666, `dim_persona` y `fact_personas` 773.932.

**Conexión.** Sin credenciales en el código. El notebook prueba, en orden:
1. `DATABASE_URL` (`postgresql://usuario:contraseña@host:puerto/base`) en variable de entorno o secretos de Colab.
2. `PG_HOST`, `PG_USER` y `PG_PASSWORD` (opcionales `PG_DB` y `PG_PORT`).
3. Una copia de las tablas en CSV dentro de `db_export/` (no necesita credenciales).
4. Si no hay nada, pide la cadena de conexión una sola vez con el texto oculto.

Si la conexión falla pero existe la copia en CSV, el notebook sigue con la copia. Cuando falla, imprime la causa probable sin mostrar la contraseña (por ejemplo, IPv6 con la conexión directa de Supabase, proyecto pausado o usuario sin el identificador del proyecto en el pooler). La copia en `db_export/` se genera una sola vez con `EXPORTAR_TABLAS = True` y se comparte por Drive, no por GitHub.

**Extracción (`read_db`).** Une `fact_personas` con `dim_persona` por `id_persona` y con `dim_geografia` por `id_geografia`, leyendo por bloques de 100.000 filas. Guarda el crudo en `db_raw.parquet`.

**Cruce con las otras fuentes (Paso 5).** Compara los hogares de esta fuente con los de las fuentes 1 y 2 (ver sección 6).

**Transformación (`transform_db`).**
- Nombres en minúscula y texto sin espacios sobrantes.
- Columnas `Decimal` de Postgres a número normal.
- `directorio` y `secuencia_p` a entero.
- El `id_hogar` interno del modelo se renombra `id_hogar_bd` y se crea `id_hogar = directorio-secuencia_p`, igual al de las otras fuentes.
- `grupo_educativo` (sin educación formal, básica, media, superior) y `ocupado`.
- Se eliminan duplicados exactos (0 en la última corrida).

**Resumen por hogar (`resumen_hogar`).** Como las otras dos fuentes son por hogar, esta se resume a una fila por hogar: `personas_db`, `ingreso_total_hogar`, `ingreso_prom_persona` y `personas_ocupadas`. Son sumas y promedios sin ponderar.

**Pobreza monetaria por departamento (`resumen_monetaria`).** Une `fact_hogares` con `dim_geografia` por `id_geografia` y calcula `pobreza_monetaria = Σ(pobre × fex_c) / Σ(fex_c) × 100` por departamento. Es la salida de esta fuente que se usa en el cruce con el IPM (guardada en `db_monetaria_dpto.parquet`). La base cubre **24 de los 33 departamentos** (faltan Arauca, Casanare, Putumayo, San Andrés, Amazonas, Guainía, Guaviare, Vaupés y Vichada). _Pendiente: suite de validación propia para este agregado (departamento sin nulos ni repetidos, porcentaje entre 0 y 100)._

**Validación.** `suite_db_personas`: `id_persona` único y sin nulos, `fex_c` positivo, edad entre 0 y 110, `ingtot` no negativo, `sexo` y `condicion_laboral` dentro de sus categorías (con tolerancia de 1 % y 5 %). `suite_db_hogares`: llave sin nulos y única, `personas_db` mayor o igual a 1.

---

## 6. Verificación cruzada entre fuentes

Los notebooks 02 y 03 comparan las llaves de los hogares entre fuentes. Resultados de la última corrida:

| Comparación | Resultado |
|---|---|
| Hogares del CSV que aparecen en el IPM | 100 % |
| Hogares del IPM que aparecen en el CSV | 99,8 % |
| `secuencia_encuesta` igual a `personas` (CSV con IPM) | 99,9 % |
| Hogares del IPM que están en la base de datos | 61,7 % |
| Hogares con el mismo número de personas (IPM con base de datos) | 21,1 % |
| Hogares del CSV que están en la base de datos | 61,7 % |

**Lectura.** El IPM y el CSV son la misma encuesta (el diccionario del DANE indica que su universo es la Encuesta de Calidad de Vida), así que se pueden unir por hogar. La base de datos de la primera entrega viene de la GEIH, una encuesta distinta: aunque el 61,7 % de las llaves coincide, solo el 21,1 % de esos hogares tiene el mismo número de personas, lo que indica coincidencias de numeración y no los mismos hogares.

**Decisión de diseño para el merge (pendiente de implementar).**
- IPM y CSV se unen **por hogar** (`id_hogar`), con el IPM como ancla.
- La base de datos (GEIH) **no se une por hogar**: se resume en agregados ponderados por departamento (hoy: pobreza monetaria; pendiente decidir si se agregan ingreso per cápita, empleo o indigencia) y se compara con el IPM por territorio.
- El IPM por hogar también se agrega a departamento (porcentaje de hogares / personas pobres, ponderado con `fex_c` / `fexp`) para cruzarlo con la GEIH.
- La unión usa `codigo_departamento` como **texto de 2 dígitos** (`"05"`, no `5`).
- Universo del cruce: **33 departamentos**. Los 9 sin GEIH se conservan con una bandera `cobertura_geih = False` y pobreza monetaria nula a propósito; las comparaciones monetaria vs. IPM usan solo los 24 con cobertura.
- Se debe confirmar la vigencia de la GEIH de la primera entrega frente al IPM 2025 y dejarla escrita como supuesto.

---

## 7. Supuestos y decisiones de transformación

1. **Factores de expansión.** Los porcentajes de hogares usan `fex_c`; los de personas, `fexp` (IPM) o `fex_c` / `fex_dpto` (GEIH). Nunca se cuenta filas.
2. **Sin imputación.** Los nulos estructurales se conservan; no se imputa ni se elimina sin justificación escrita.
3. **Atípicos.** Se conservan, porque los factores de expansión varían por el diseño de la muestra.
4. **Tolerancias.** Las reglas de Great Expectations usan `mostly` donde se espera algún caso fuera de las reglas (códigos sin describir, edades extremas).
5. **Llave común.** `id_hogar = directorio-secuencia_p` en las tres fuentes.
6. **Compuerta de calidad.** El archivo limpio de cada fuente solo se guarda si la validación pasa.
7. **Credenciales.** Nunca en el código ni en el repositorio.
8. **Departamentos con pocos hogares** (menos de 30 en la muestra) se tratan como estimaciones poco confiables.
9. **Código de departamento como texto.** Siempre 2 dígitos, para que el `5` de Antioquia no rompa la unión con `"05"`.
10. **GEIH e IPM no se unen por hogar.** El 21,1 % de coincidencia en el número de personas muestra que son hogares distintos; el cruce es territorial.
11. **Cobertura de la GEIH.** Solo 24 de 33 departamentos. Los otros 9 se conservan con la bandera `cobertura_geih`; no se imputa su pobreza monetaria.
12. **`resumen_hogar` (notebook 03)** son sumas y promedios sin ponderar y queda solo como apoyo del EDA; el cruce usa `resumen_monetaria`, que sí pondera con `fex_c`.

---

## 8. Calidad de datos y registros

Cada validación usa una `ExpectationSuite` de Great Expectations 1.x y deja:
- `reports/validation_log.csv`: una fila por regla (fecha, suite, regla, parámetros, resultado y porcentaje fuera de la regla), compartido por los tres notebooks.
- `reports/ge_<suite>_notebook.json`: resumen de cada corrida.
- `reports/hallazgos_api.md`, `hallazgos_csv.md` y `hallazgos_db.md`: resumen automático del EDA.
- `reports/eda_*.png`: gráficos del EDA de cada fuente.

**Resumen de las suites de Great Expectations**

| Suite | Fuente | Qué valida | Tolerancias (`mostly`) |
|---|---|---|---|
| `suite_api_ipm` | API (IPM) | Llaves e indicadores sin nulos; indicadores y `pobre` en {0, 1}; `id_hogar` único; `ipm` en [0, 1]; `personas` en [1, 30]; `n_privaciones` en [0, 15]; factores > 0; departamentos del catálogo; consistencia de `pobre`, `ipm` y `fexp` | 99 % en `ipm` y `fexp` |
| `suite_csv` | CSV | Llaves sin nulos y únicas; rangos de `secuencia_p` y `secuencia_encuesta`; códigos del diccionario; sin nulos (21 reglas en la última corrida) | 99 % en códigos, 95 % en nulos |
| `suite_db_personas` | BD | `id_persona` único, `fex_c` > 0, edad en [0, 110], `ingtot` ≥ 0, `sexo` y `condicion_laboral` en sus categorías | 99 % / 95 % |
| `suite_db_hogares` | BD | Llave sin nulos y única, `personas_db` ≥ 1 | ninguna (5 reglas) |

Cada notebook escribe en el mismo `validation_log.csv`; antes de entregar, conviene borrar el archivo y volver a ejecutar los tres para que el registro quede limpio y sin corridas de prueba repetidas.

---

## 9. Cómo ejecutar

**Requisitos:** Google Colab, o Python 3.10+ en local, con:

```bash
pip install "great-expectations>=1.0,<2" pandas pyarrow requests matplotlib scipy sqlalchemy psycopg2-binary
```

**Carpeta de trabajo.** En Colab, `Mi unidad/Colab Notebooks/etl_proyecto/`; en local, `./etl_proyecto/`. Se crean `staging/` (archivos intermedios), `reports/` (registros y gráficos) y `db_export/` (copia de la base). Esta carpeta de trabajo está fuera del repositorio: al subir a GitHub solo se copian `reports/` (sin datos personales) y los notebooks.

**Orden.**
1. `01_fuente_api_ipm_def.ipynb` — primero, porque deja `ipm_api_clean.parquet`, que usan los otros dos para comparar.
2. `02_fuente_csv_def.ipynb` — requiere `viviendas_pdtos.csv` en la carpeta de trabajo o en `CSV_PATH`; si no está, Colab pide subirlo.
3. `03_fuente_base_de_datos_def.ipynb` — requiere conexión a Supabase o la copia en `db_export/`.

**La API.** El notebook 01 necesita que el contenedor Docker esté encendido y accesible en `API_URL`. _[Completar: enlace al repositorio o Dockerfile de la API, comando para levantarla y qué hacer si el servidor está apagado.]_

**Verificación rápida.** Después de ejecutar cada notebook debe aparecer `PASS` en la línea `[GE] ... -> PASS` y el mensaje `Guardado: ...`. Si aparece `FAIL`, revisar las reglas impresas debajo; el archivo limpio no se guarda.

**Variables de configuración** (entorno o secretos de Colab): `API_URL` (opcional), `CSV_PATH` (opcional), `DATABASE_URL` o `PG_HOST`, `PG_USER`, `PG_PASSWORD` (opcionales `PG_DB`, `PG_PORT`). Con Supabase desde Colab se recomienda la cadena del *Session pooler* (la conexión directa usa IPv6).

---

## 10. Estructura del repositorio

```
.
├── README.md
├── .gitignore
├── notebooks/
│   ├── 01_fuente_api_ipm_def.ipynb
│   ├── 02_fuente_csv_def.ipynb
│   ├── 03_fuente_base_de_datos_def.ipynb
│   └── (Entrega 1) ETL_EDA_Hogares_DEF.ipynb, ETL_personas_DEF.ipynb
├── dags/                  # Airflow (pendiente)
├── reports/               # validation_log.csv, resúmenes de Great Expectations, hallazgos, gráficos del EDA
├── visualizaciones/       # gráficos del dashboard final (pendiente)
├── diagramas/             # arquitectura y modelo dimensional
└── documentos/
```

El `.gitignore` debe excluir como mínimo:

```
.env
*.parquet
*.csv          # salvo reports/validation_log.csv si se quiere versionar
etl_proyecto/staging/
etl_proyecto/db_export/
```

Antes de subir los notebooks, **borrar las salidas de las celdas**: la conexión a la base imprime el usuario del pooler (contiene el identificador del proyecto de Supabase) y el servidor, y el notebook 02 deja tablas con datos de hogares. Se recomienda además quitar del código la línea que imprime el usuario y el largo de la contraseña.

---

## 11. Diseño previsto del DAG de Airflow  _[pendiente de implementar]_

```
read_api ──► transform_api ──┐
read_csv ──► transform_csv ──┼─► merge ─► validar_merge (GE) ─► cargar_dw ─► visualizar
read_db  ──► transform_db  ──┘                 │
                                               └─ si falla: no se carga (las tareas siguientes quedan sin ejecutar)
```

- Tres tareas de extracción independientes y en paralelo, programadas a intervalo fijo (por ejemplo `@daily`).
- Cada tarea deja su resultado en `staging/` en lugar de pasar DataFrames por XCom.
- La validación sobre el dataset unido es la compuerta de calidad; sus resultados se escriben en `validation_log.csv` y, si es posible, en una tabla del warehouse.
- La carga es una recarga completa dentro de una transacción (todo o nada).

---

## 12. Pendiente para completar la Entrega 2

1. Notebook de merge: IPM con CSV por hogar y agregados de la GEIH por departamento.
2. Suite de Great Expectations sobre el dataset unido (llave única, cobertura mínima, coherencia entre fuentes, códigos válidos).
3. Modelo dimensional y carga al warehouse (reutilizando `dim_geografia` y `dim_tiempo` de la Entrega 1), con compuerta: no se carga si la validación falla. Nota: `dim_geografia` tiene 76 filas (departamento, dominio, ciudad y clase), más fina que un departamento; para los hechos por departamento hace falta agregar `codigo_departamento` y una vista de departamentos únicos, o una dimensión de departamento derivada.
4. DAG de Airflow: tres ramas en paralelo, merge, validación, carga y visualización.
5. Visualizaciones y dashboard sobre el warehouse.
6. Diagrama de arquitectura actualizado (corregir el diagrama de la Entrega 1: tenía dos entradas sueltas que no existen en el modelo real) e informe técnico.

### Relación con el entregable

| Punto del entregable | Dónde está | Estado |
|---|---|---|
| Objetivos refinados | Sección 1 | Listo |
| Tres fuentes y tres extracciones | Sección 5, notebooks 01 a 03 | Listo como notebooks; falta convertirlas en tareas de Airflow |
| EDA | Notebooks 01 a 03 y `reports/hallazgos_*.md` | Listo |
| Transformación y merge | Sección 5 (por fuente) y sección 6 (diseño del merge) | Transformaciones listas; merge pendiente |
| Great Expectations integrado al DAG | Sección 8 (suites por fuente) | Suites por fuente listas; suite del merge y DAG pendientes |
| Modelo de datos y carga | Sección 12, punto 3 | Pendiente |
| Visualizaciones | Sección 12, punto 5 | Pendiente |
| API: detalles y por qué se eligió | Sección 5.2 | Listo (con limitaciones documentadas) |
| Diagrama de arquitectura | Sección 4 (texto); falta versión gráfica | Parcial |
| Supuestos y decisiones | Sección 7 | Listo |
| `.gitignore`, README, visualizaciones | Sección 10 | Parcial |
