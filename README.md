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

**Objetivo refinado de la Entrega 2.** Automatizar la ingesta de tres fuentes y compararlas entre sí. La pregunta central es: *¿coincide el mapa de pobreza monetaria (GEIH) con el de pobreza multidimensional (IPM)? ¿Qué departamentos se ven distintos según la medida y qué condiciones de vivienda los acompañan?* Para responderla, cada fuente cumple un propósito y las tres se resumen a **una fila por departamento**:

| Fuente | Qué aporta al objetivo |
|---|---|
| **API (IPM)** | Pobreza multidimensional por hogar (15 indicadores de privación, índice `ipm`, marca `pobre`, factores de expansión). En el merge se resume en la incidencia del IPM y el IPM promedio por departamento. |
| **CSV (viviendas)** | Condiciones de la vivienda que están detrás de cuatro indicadores del IPM. En el merge se resumen en el porcentaje de hogares sin acueducto, sin alcantarillado, con piso de tierra y en zona rural dispersa. |
| **Base de datos (Entrega 1)** | Pobreza monetaria (GEIH) por departamento, ponderada con el factor de expansión. |

**Resultados que debe entregar el pipeline (Entrega 2).**
1. Una comparación por departamento entre pobreza monetaria (GEIH) y pobreza multidimensional (IPM): cuánto se parecen y dónde se separan.
2. Qué condiciones de vivienda (sin acueducto, sin alcantarillado, piso de tierra, zona rural dispersa) acompañan a la pobreza multidimensional de cada departamento.
3. Un dataset final, `merge_departamento` (una fila por departamento), validado y cargado en una base relacional.
4. Un reporte con cifras y gráficas que salen **solo** de esa tabla cargada (consultas SQL), y una aplicación en Streamlit que lea el mismo resultado _(pendiente)_.

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
| Pobreza monetaria agregada por departamento (GEIH, ponderada con `fex_c`) | Listo (notebook 03, `db_monetaria_dpto.parquet`); se valida de forma indirecta en la suite del merge |
| Merge por departamento | Listo (`merge_completote.ipynb`): 33 filas x 16 columnas |
| Validación con Great Expectations sobre el dataset unido | Listo (`suite_merge_departamento`, 24 reglas) |
| DAG de Apache Airflow | Listo (`pobreza_dag`, 10 tareas, `@daily`) |
| Carga a base relacional | Listo: tabla `merge_departamento`. SQLite por defecto; PostgreSQL definiendo `DESTINO_URL` |
| Reporte con consultas SQL sobre la tabla cargada | Listo en el notebook (3 gráficas); falta completar la conclusión de la gráfica 3 |
| Streamlit | Pendiente |
| Modelo dimensional documentado y diagrama final | Pendiente / parcial (el diagrama del pipeline se genera en el notebook; falta subirlo al repositorio) |

La sección 12 relaciona cada punto del entregable con su estado.
---
## 3. Stack tecnológico

| Capa | Tecnología | Para qué se usa |
|---|---|---|
| Entorno de desarrollo | Google Colab + Google Drive | Ejecución en la nube, trabajo en equipo y archivos compartidos. |
| Procesamiento | Python (pandas, numpy, pyarrow, scipy) | Lectura, limpieza, transformación y archivos Parquet. |
| Fuente API | Servidor propio en Docker + requests/urllib3 | Servicio HTTP con el IPM; peticiones con reintentos y espera creciente. |
| Base de datos de origen | PostgreSQL (Supabase) | Contiene el modelo de la Entrega 1 (fuente 3). |
| Base de datos de destino | SQLite (pruebas) o PostgreSQL (`DESTINO_URL`) | Guarda la tabla final `merge_departamento`. |
| Conexión Python ↔ BD | SQLAlchemy + psycopg2-binary | Lectura de tablas por bloques y carga del resultado. |
| Calidad de datos | Great Expectations 1.x (`>=1.0,<2`) | Suites de validación, con registro de cada regla. |
| Orquestación | Apache Airflow (entorno aislado en Colab) | DAG `pobreza_dag`: lectura, transformación, merge, calidad y carga. |
| Gráficas | matplotlib | EDA y reporte final. |
| Aplicación | Streamlit | Pendiente: lee `merge_departamento`. |
| Control de versiones | Git / GitHub | Código y documentación. |
---
## 4. Arquitectura y flujo

```
  API Docker (IPM)          CSV (viviendas_pdtos.csv)      Supabase (modelo Entrega 1)
        │                            │                              │
    read_api                     read_csv                        read_db
        │                            │                              │
  transform_api                transform_csv                  transform_db
  + validar (GE)               + validar (GE)                + validar (GE)
        │                            │                       (personas, hogares)
        │                            │                              │
        └─────────────┬──────────────┴──────────────┬───────────────┘
                      ▼                              
        MERGE por departamento (codigo_departamento, outer join)
        IPM + viviendas + pobreza monetaria → 1 fila por departamento
                      │
                      ▼
          quality_check: Great Expectations sobre el merge
                      │   (si falla, no se carga nada)
                      ▼
          load: tabla merge_departamento (base relacional)
                      │
                      ▼
          store: merge_departamento.csv (leído de la tabla)
                      │
                      ▼
          Reporte SQL y gráficas  ·  Streamlit [pendiente]

     Orquestación: Apache Airflow, DAG `pobreza_dag`, @daily
```

El diagrama gráfico se genera en el notebook del merge (`reports/arquitectura_pipeline.png`).
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

**Qué es.** Un servidor propio, desplegado con Docker, que expone por HTTP el archivo `HOGARES (DEPARTAMENTAL).csv`. Ese archivo contiene el Índice de Pobreza Multidimensional (IPM) publicado por el DANE, con un registro por hogar: los 15 indicadores de privación (0 o 1), el `ipm`, la marca `pobre`, `personas` y los factores de expansión `fex_c` (hogares) y `fexp` (personas). **No es una API oficial del DANE:** los datos son públicos y el servicio lo construimos nosotros.

**Endpoint.** `GET http://201.32.130.152/api/productos`. Es de acceso libre, sin llave ni autenticación, y devuelve la lista de registros en JSON. El nombre `/productos` no refleja su contenido.

```python
import requests
r = requests.get("http://201.32.130.152/api/productos", timeout=120)
print(r.status_code, len(r.json()))
```

**Por qué se eligió.**
- El IPM mide la pobreza por privaciones (educación, salud, vivienda, etc.) y complementa a la pobreza monetaria de la base de datos y a las condiciones de vivienda del CSV.
- Las tres fuentes comparten el departamento, que es la llave para unirlas en el merge.
- Al servirlo como API, el pipeline lo extrae por HTTP y de forma programada, como pide el proyecto.

**Cómo lo consume el pipeline.** La tarea `read_api` descarga los registros con `requests`, con un tiempo de espera de 120 s y reintentos automáticos en la sesión HTTP, y sigue la paginación si la respuesta trae un enlace a la siguiente página. Si el servidor responde con error, la tarea falla y se reintenta una vez (10 s después). Los datos descargados se validan en `transform_api` con la suite `suite_api_ipm` (Great Expectations).

**Disponibilidad.** El servidor está alojado en un servicio de pago y permanece encendido. Si llegara a fallar, `read_api` falla y el DAG se detiene antes de `load`, por lo que la tabla final no se modifica con datos incompletos.

**Limitaciones.** La conexión es HTTP sin cifrar y sin autenticación, algo aceptable porque los datos son públicos y no contienen información personal. El IPM del DANE se publica por periodos, así que la ejecución diaria vuelve a leer la misma publicación hasta que se actualice el archivo del servidor.

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

**Resumen por departamento.** Se calcula el IPM promedio ponderado por departamento: `ipm_ponderado = Σ(ipm × fex_c) / Σ(fex_c)`, en escala 0 a 1. Esta idea es la que usa el merge para `ipm_promedio` y para la incidencia del IPM (% de hogares pobres ponderado con `fex_c`). (Un promedio de `ipm × fex_c` sin dividir por la suma de pesos no sirve: mezcla el nivel de pobreza con el tamaño de los factores de expansión.)

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

**Comparación con la fuente 1.** Cruce por `id_hogar` con `ipm_api_clean.parquet` (si ya existe; es solo una verificación, el merge final no une por hogar): hogares en cada fuente, porcentaje de cobertura y si `secuencia_encuesta` coincide con `personas`. Además muestra la pobreza multidimensional según cada variable de vivienda y la relación entre cada respuesta cruda y su indicador del IPM.

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

**Pobreza monetaria por departamento (`resumen_monetaria`).** Une `fact_hogares` con `dim_geografia` por `id_geografia` y calcula `pobreza_monetaria = Σ(pobre × fex_c) / Σ(fex_c) × 100` por departamento. Es la salida de esta fuente que se usa en el cruce con el IPM (guardada en `db_monetaria_dpto.parquet`). La base cubre **24 de los 33 departamentos** (faltan Arauca, Casanare, Putumayo, San Andrés, Amazonas, Guainía, Guaviare, Vaupés y Vichada). Este agregado no tiene suite propia; la suite del merge valida que `pobreza_monetaria` esté entre 0 y 100 y que el departamento sea único.

**Validación.** `suite_db_personas`: `id_persona` único y sin nulos, `fex_c` positivo, edad entre 0 y 110, `ingtot` no negativo, `sexo` y `condicion_laboral` dentro de sus categorías (con tolerancia de 1 % y 5 %). `suite_db_hogares`: llave sin nulos y única, `personas_db` mayor o igual a 1.


### 5.5 Merge por departamento (`merge_completote.ipynb`)

**Qué hace.** Lee los archivos limpios de los notebooks 01, 02 y 03, resume cada fuente a una fila por departamento y las une por `codigo_departamento` (texto de 2 dígitos) con un `outer join`. En la Parte 2 del mismo notebook el proceso completo se automatiza como DAG de Airflow (sección 11).

| Fuente | Columnas que aporta | Cómo se calcula |
|---|---|---|
| IPM (API) | `hogares_ipm`, `incidencia_ipm`, `ipm_promedio` | Hogares de la muestra; % de hogares pobres ponderado con `fex_c`; IPM promedio ponderado con `fex_c` (escala 0 a 1) |
| CSV (viviendas) | `hogares_csv`, `pct_sin_acueducto`, `pct_sin_alcantarillado`, `pct_piso_tierra`, `pct_rural_disperso` | Porcentajes de hogares de la muestra, **sin ponderar** (el archivo no trae factor de expansión) |
| Base de datos | `hogares_db`, `pobreza_monetaria` | % de hogares pobres ponderado con `fex_c` |
| Calculadas en el merge | `en_ipm`, `en_csv`, `en_db`, `diferencia_ipm_monetaria` | Marcas de origen; incidencia del IPM menos pobreza monetaria, en puntos porcentuales |

Resultado: **33 filas x 16 columnas**, una por departamento, ordenadas por código.

**Resultados de la última corrida.**

| Cifra | Valor |
|---|---|
| Departamentos en el resultado | 33 |
| Departamentos con las tres fuentes | 24 |
| Departamentos usados en las comparaciones (con al menos 30 hogares en la muestra del IPM) | 24 |
| Departamentos sin pobreza monetaria | Arauca, Casanare, Putumayo, San Andrés y Providencia, Amazonas, Guainía, Guaviare, Vaupés y Vichada |
| Correlación entre incidencia del IPM y pobreza monetaria | 0,92 |
| Mayor incidencia del IPM | La Guajira (34,5 % de los hogares) |
| Mayor pobreza monetaria | Chocó (54,4 % de los hogares) |
| Diferencia más alta (IPM menos pobreza monetaria) | Caldas (−6,7 puntos) |
| Diferencia más baja | Chocó (−26,8 puntos) |

**Cómo leer la diferencia.** Es IPM menos pobreza monetaria. Un valor negativo significa que el departamento sale menos pobre por el IPM que por ingresos. En la última corrida es negativa en todos los departamentos comparados, así que la "diferencia más alta" es la más cercana a cero y la "más baja" es la brecha más grande entre las dos medidas. Las cifras describen los datos; no explican las causas.

**Validación (`suite_merge_departamento`, 24 reglas).** `codigo_departamento` sin nulos, único y dentro del catálogo; porcentajes entre 0 y 100; IPM promedio entre 0 y 1; columnas del IPM y del CSV sin nulos; y `pobreza_monetaria` con hasta 30 % de nulos (`mostly=0.7`), porque faltan los 9 departamentos que no están en la base de datos.

**Salidas.** `staging/merge_departamento.parquet` y `.csv` (solo si la validación pasa), la tabla `merge_departamento` en la base de destino y el reporte (3 gráficas y 9 cifras) con consultas SQL a esa tabla.

**Reporte.** Todas las cifras y gráficas del Paso 17 salen de consultas SQL a la tabla cargada, no de los archivos del merge, es decir, de datos que ya pasaron la validación. Las gráficas se guardan en `reports/`.
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

**Decisión de diseño del merge (implementada).**
- **Las tres fuentes se unen por departamento, no por hogar.** El CSV y el IPM sí comparten hogares, pero la base de datos (GEIH) no, y para que el merge tenga una sola lógica resumimos las tres a departamento.
- El IPM y el CSV se resumen a porcentajes por departamento (el IPM con `fex_c`, el CSV sin ponderar); la base de datos se resume en la pobreza monetaria ponderada con `fex_c`.
- La unión usa `codigo_departamento` como **texto de 2 dígitos** (`"05"`, no `5`) y un `outer join`, para no perder departamentos de ninguna fuente.
- **Universo del cruce: 33 departamentos.** Los 9 sin GEIH se conservan con `en_db = False` y pobreza monetaria nula a propósito; las comparaciones entre medidas usan los 24 que están en las tres fuentes y tienen al menos 30 hogares en la muestra del IPM (`MIN_HOGARES`).
- Queda como supuesto la vigencia de la GEIH de la primera entrega frente al IPM 2025.
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
11. **Cobertura de la GEIH.** Solo 24 de 33 departamentos. Los otros 9 se conservan con `en_db = False`; no se imputa su pobreza monetaria.
12. **`resumen_hogar` (notebook 03)** son sumas y promedios sin ponderar y queda solo como apoyo del EDA; el cruce usa `resumen_monetaria`, que sí pondera con `fex_c`.
13. **CSV sin ponderar.** El archivo de viviendas no trae factor de expansión; sus porcentajes son de los hogares de la muestra y no se comparan como si fueran cifras poblacionales.
14. **Comparaciones mínimas.** Solo se usan departamentos con al menos 30 hogares en la muestra del IPM (`MIN_HOGARES = 30`).
15. **Diferencia entre medidas.** `diferencia_ipm_monetaria` es IPM menos pobreza monetaria, en puntos porcentuales; mezcla dos conceptos distintos de pobreza y se interpreta como descripción, no como error de una de las medidas.
16. **Calidad como compuerta.** Si falla una validación de `transform_*` o de `quality_check`, el DAG se detiene y no se carga nada en la tabla final.
17. **Recarga completa.** La tabla final se reemplaza completa en cada ejecución (`if_exists='replace'`), así que repetir el DAG no duplica filas.
18. **Base de destino.** Si no se define `DESTINO_URL`, la carga va a SQLite (útil para pruebas); para PostgreSQL se define `DESTINO_URL` con su cadena de conexión.
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
| `suite_merge_departamento` | Merge | `codigo_departamento` sin nulos, único y del catálogo; porcentajes en [0, 100]; `ipm_promedio` en [0, 1]; columnas del IPM y del CSV sin nulos; hogares ≥ 1 (24 reglas) | 70 % en `pobreza_monetaria` |

En la última ejecución del DAG todas las reglas se cumplieron: `suite_api_ipm` 54 de 54, `suite_csv` 21 de 21, `suite_db_personas` 12 de 12, `suite_db_hogares` 5 de 5 y `suite_merge_departamento` 24 de 24. Cuando las validaciones corren dentro del DAG, el resumen queda en `reports/ge_<suite>_dag.json`.

Cada notebook escribe en el mismo `validation_log.csv`; antes de entregar, conviene borrar el archivo y volver a ejecutar los tres para que el registro quede limpio y sin corridas de prueba repetidas.

---
## 9. Cómo ejecutar

**Requisitos:** Google Colab, o Python 3.10+ en local, con:

```bash
pip install "great-expectations>=1.0,<2" pandas pyarrow requests matplotlib scipy sqlalchemy psycopg2-binary
```

**Carpeta de trabajo.** En Colab, `Mi unidad/Colab Notebooks/etl_proyecto/`; en local, `./etl_proyecto/`. Se crean `staging/` (archivos intermedios), `reports/` (registros y gráficos) y `db_export/` (copia de la base). Esta carpeta de trabajo está fuera del repositorio: al subir a GitHub solo se copian `reports/` (sin datos personales), los notebooks y el DAG.

**Parte 1: notebooks por fuente y merge.**
1. `01_fuente_api_ipm_def.ipynb`: primero, porque deja `ipm_api_clean.parquet`, que usan los otros dos para comparar.
2. `02_fuente_csv_def.ipynb`: requiere `viviendas_pdtos.csv` en la carpeta de trabajo o en `CSV_PATH`; si no está, Colab pide subirlo.
3. `03_fuente_base_de_datos_def.ipynb`: requiere conexión a Supabase o la copia en `db_export/`. Debe guardar también `db_monetaria_dpto.parquet`.
4. `merge_completote.ipynb`, Pasos 1 a 8: lee los archivos limpios de los tres notebooks anteriores y deja `merge_departamento.parquet` y `.csv`.

**Parte 2: pipeline con Airflow** (`merge_completote.ipynb`, Pasos 9 a 19). El DAG no usa los archivos de los notebooks 01 a 03: lee las tres fuentes originales.
1. Paso 1 (configuración), Paso 9 (entorno de Airflow, tarda unos minutos) y Paso 10 (variables y secretos).
2. Paso 14: iniciar Airflow. Paso 15: ejecutar el DAG y esperar a que termine en `SUCCESS`.
3. Paso 16: revisar el registro de validación, la tabla final y los archivos guardados. Paso 17: reporte.
4. Paso 19: copiar `dags/pobreza_dag.py` al repositorio.

**La API.** El servidor Docker está encendido de forma permanente y responde en `API_URL`. _[Completar: enlace al repositorio o Dockerfile de la API y el comando para levantarla.]_

**Verificación rápida.** Después de ejecutar cada notebook debe aparecer `PASS` en la línea `[GE] ... -> PASS` y el mensaje `Guardado: ...`. Si aparece `FAIL`, revisar las reglas impresas debajo; el archivo limpio no se guarda. En el DAG, si una validación falla, la tarea queda en `failed` y las siguientes no se ejecutan.

**Variables de configuración** (entorno o secretos de Colab):

| Variable | Para qué sirve |
|---|---|
| `DATABASE_URL` | Base de datos de origen (Supabase). También se aceptan `PG_HOST`, `PG_USER`, `PG_PASSWORD` (opcionales `PG_DB`, `PG_PORT`). |
| `API_URL` (opcional) | Dirección de la API; por defecto `http://201.32.130.152/api/productos`. |
| `CSV_PATH` (opcional) | Ruta de `viviendas_pdtos.csv`. |
| `DESTINO_URL` (opcional) | Base donde se carga `merge_departamento`; por defecto SQLite. |
| `DAG_SCHEDULE` | `@daily` por defecto; en Colab se pone `none` para lanzar el DAG a mano. |

Con Supabase desde Colab se recomienda la cadena del *Session pooler* (la conexión directa usa IPv6).
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
│   ├── merge_completote.ipynb
│   └── (Entrega 1) ETL_EDA_Hogares_DEF.ipynb, ETL_personas_DEF.ipynb
├── dags/
│   └── pobreza_dag.py         # DAG de Airflow (10 tareas, @daily)
├── reports/                   # validation_log.csv, resúmenes de Great Expectations, hallazgos, gráficas y diagrama
├── diagramas/                 # arquitectura y modelo dimensional
└── documentos/
```

El `.gitignore` debe excluir como mínimo:

```
.env
*.parquet
*.pkl
*.csv          # salvo reports/validation_log.csv y merge_departamento.csv si se quieren versionar
etl_proyecto/
airflow/
```

Antes de subir los notebooks, **borrar las salidas de las celdas**: la conexión a la base imprime el usuario del pooler (contiene el identificador del proyecto de Supabase) y el servidor, y el notebook 02 deja tablas con datos de hogares. Se recomienda además quitar del código la línea que imprime el usuario y el largo de la contraseña.
---
## 11. DAG de Airflow (`pobreza_dag`)

```
read_api ──► transform_api ──┐
read_csv ──► transform_csv ──┼─► merge ─► quality_check ─► load ─► store
read_db  ──► transform_db  ──┘
```

El DAG vive en `dags/pobreza_dag.py` (se genera desde `merge_completote.ipynb`) y trae todas las funciones, así que no depende de los notebooks.

| Tarea | Qué hace | Reintentos |
|---|---|---|
| `read_api` | Descarga el IPM de la API, con paginación y reintentos | 1 (10 s) |
| `read_csv` | Lee el CSV de viviendas | 1 (10 s) |
| `read_db` | Lee personas (hechos, dimensión y geografía) y hogares con pobreza monetaria | 1 (10 s) |
| `transform_api`, `transform_csv`, `transform_db` | Limpian cada fuente y la validan con Great Expectations; si no cumple, se detienen | 0 |
| `merge` | Resume cada fuente a una fila por departamento y las une; revisa que no haya departamentos repetidos | 0 |
| `quality_check` | Valida el resultado del merge | 0 |
| `load` | Guarda `merge_departamento` en la base relacional y compara el número de filas con el del dataframe | 1 (10 s) |
| `store` | Genera `merge_departamento.csv` leyendo la tabla | 1 (10 s) |

**Programación.** El DAG `pobreza_dag` se ejecuta una vez al día (`schedule="@daily"`, a las 00:00 UTC) con `catchup=False`, es decir, no se recuperan los días anteriores. Cada ejecución vuelve a leer las tres fuentes y reemplaza la tabla `merge_departamento`, así que se puede repetir sin duplicar datos. En Colab la variable `DAG_SCHEDULE=none` solo desactiva el horario para lanzarlo a mano; en el repositorio queda `@daily`. En Colab el DAG solo corre mientras la sesión esté abierta; para una ejecución diaria continua, Airflow debe estar siempre encendido (por ejemplo, en Docker).

**Decisiones de diseño.**
- Cada tarea guarda su resultado en un archivo temporal (`staging`) y la siguiente lo lee; no se pasan dataframes por XCom.
- Las tareas de lectura, `load` y `store` se reintentan una vez porque el fallo puede ser pasajero (API o base lentas). Las tareas `transform_*`, `merge` y `quality_check` no se reintentan: si los datos están mal, repetir no lo arregla.
- La validación con Great Expectations es la compuerta de calidad: si falla una regla, la tarea falla, las siguientes no se ejecutan y la tabla final no se modifica.
- `max_active_runs=1`: no corren dos ejecuciones a la vez.
- En Colab las tres ramas corren una después de la otra (Airflow usa SQLite), no en paralelo.
---
## 12. Pendiente para completar la Entrega 2

1. Streamlit: aplicación que lea `merge_departamento` (la tabla o el CSV) y muestre las gráficas.
2. Completar la conclusión de la gráfica 3 (hogares sin acueducto contra incidencia del IPM) en el reporte.
3. Subir al repositorio `dags/pobreza_dag.py`, las gráficas y el diagrama del pipeline (`reports/arquitectura_pipeline.png`).
4. Confirmar que la carga final se hace en PostgreSQL (definiendo `DESTINO_URL`) y no en SQLite, y dejarlo escrito.
5. Modelo de datos del warehouse: la Entrega 2 carga una sola tabla por departamento; si se quiere un esquema dimensional, reutilizar `dim_geografia` (76 filas, más fina que un departamento) agregando `codigo_departamento` y una vista de departamentos únicos.
6. Diagrama de arquitectura final (corregir el diagrama de la Entrega 1, que tenía dos entradas sueltas que no existen en el modelo real) e informe técnico.
7. Completar en el README el enlace al repositorio o Dockerfile de la API.

### Relación con el entregable

| Punto del entregable | Dónde está | Estado |
|---|---|---|
| Objetivos refinados | Sección 1 | Listo |
| Tres fuentes y tres extracciones en Airflow | Secciones 5 y 11; tareas `read_api`, `read_csv`, `read_db` | Listo |
| EDA | Notebooks 01 a 03 y `reports/hallazgos_*.md` | Listo |
| Transformación y merge | Secciones 5 y 5.5; tareas `transform_*` y `merge` | Listo |
| Great Expectations integrado al DAG | Secciones 8 y 11; 5 suites | Listo |
| Carga a base relacional | Sección 5.5; tarea `load` | Listo (verificar destino PostgreSQL) |
| Modelo de datos documentado | Sección 12, punto 5 | Pendiente |
| Visualizaciones | Sección 5.5 (reporte SQL, 3 gráficas); Streamlit pendiente | Parcial |
| API: detalles y por qué se eligió | Sección 5.2 | Listo |
| Diagrama de arquitectura | Sección 4 (texto) y `reports/arquitectura_pipeline.png` | Parcial |
| Supuestos y decisiones | Sección 7 | Listo |
| `.gitignore`, README, visualizaciones | Sección 10 | Parcial |