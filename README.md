Pobreza, Pobreza Extrema e Indigencia en Colombia — Proyecto ETL (G51)
Universidad Autónoma de Occidente — Facultad de Ingeniería y Ciencias Básicas
Programa: Ingeniería de Datos e Inteligencia Artificial
Curso: ETL (G51)
Integrantes:
- Sebastián Alejandro Jiménez Solís
- Laura Valentina Loaiza Díaz
- Santiago Lourido Rodríguez
- Carlos Fabián Córdoba

Visión General y Objetivos del Proyecto
Analizar la distribución, profundidad y determinantes sociodemográficos de la pobreza monetaria, pobreza extrema (indigencia) y pobreza multidimensional (IPM) en los hogares y personas en Colombia, a partir de datos de encuestas oficiales (GEIH / DANE) y la API de Pobreza Multidimensional.
El proyecto abarca el ciclo de vida completo de un pipeline de ingeniería de datos:
Extracción: Ingesta de archivos CSV de la GEIH y peticiones resilientes a la API de IPM en Docker.
Transformación & Arquitectura de Medallón: Limpieza, estandarización, imputación y enriquecimiento estructurado en capas (Bronce ➔ Plata ➔ Oro).
Modelado Dimensional (Esquema de Galaxia / Constelación): Tablas de hechos (fact_hogares, fact_personas, fact_ipm) integradas mediante dimensiones compartidas (dim_geografia, dim_tiempo, dim_hogar) y específicas (dim_persona).
Carga & Almacenamiento: Carga a motor relacional PostgreSQL (Supabase) mediante SQLAlchemy y zona de Staging en Parquet/CSV.
Calidad de Datos: Validación continua mediante suites de pruebas automatizadas con Great Expectations.
Orquestación: Automatización del flujo mediante Apache Airflow.
Analítica & Visualización (EDA): Consultas SQL sobre el esquema de galaxia con cálculos ponderados por factor de expansión (fex_c / fexp) y visualizaciones integradas.
 Preguntas de Negocio / Investigación
Nivel Hogar (GEIH - Entrega 1):

 Stack Tecnológico


Capa
Tecnología
Justificación / Descripción
Entorno de desarrollo
Google Colab + Google Drive
Ejecución interactiva en la nube, fácil colaboración y lectura directa desde Drive.
Procesamiento / ETL
Python (pandas, numpy, pyarrow)
Limpieza, estructuración, imputación y exportación de grandes volúmenes de datos.
Ingesta de Red
requests, urllib3
Ingesta HTTP resiliente desde la API Docker con reintentos automáticos y backoff exponencial.
Base de datos / DW
PostgreSQL (Supabase)
Motor relacional cloud para alojar el Data Warehouse (Esquema de Galaxia).
Conexión Python ↔ DB
SQLAlchemy + psycopg2-binary
Métodos create_engine(), to_sql() y read_sql() para transferencia y consulta directa de datos.
Calidad de Datos
Great Expectations (>=1.0,<2)
Validación automatizada de tipos, valores nulos, rangos e integridad referencial.
Orquestación
Apache Airflow
Automatización, modularización y control de dependencias mediante DAGs.
Visualización / EDA
matplotlib, seaborn
Gráficos analíticos construidos a partir de consultas SQL contra Supabase.
Control de versiones
Git / GitHub
Gestión del código fuente del repositorio.

 Arquitectura y Flujo del Pipeline


  Google Drive (Hogares.csv, Personas.csv)      API Docker IPM (HTTP Endpoint)
                  │                                         │
                  ▼                                         ▼
  ┌───────────────────────────────────────────────────────────────────────────┐
  │ EXTRACCIÓN E INGESTA                                                      │
  │  • Lectura de CSV con validación de claves únicas y rangos (GEIH).       │
  │  • Session HTTP con Retry y timeout de 120s para API IPM.                 │
  └───────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
  ┌───────────────────────────────────────────────────────────────────────────┐
  │ TRANSFORMACIÓN (Arquitectura de Medallón)                                 │
  │  • Bronce: Datos en crudo.                                               │
  │  • Plata: Normalización de nombres, decodificación DANE y categorías.     │
  │  • Oro: Indicadores derivados (brecha pobreza, log_ingreso, n_privaciones).│
  └───────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
  ┌───────────────────────────────────────────────────────────────────────────┐
  │ MODELO DIMENSIONAL — Esquema de Galaxia (Constelación)                    │
  │  • Dimensiones compartidas: dim_geografia, dim_tiempo, dim_hogar          │
  │  • Dimensión específica: dim_persona                                      │
  │  • Tablas de Hechos: fact_hogares, fact_personas, fact_ipm                │
  └───────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
  ┌───────────────────────────────────────────────────────────────────────────┐
  │ CARGA, CALIDAD Y ORQUESTACIÓN                                             │
  │  • Carga a Supabase (PostgreSQL) mediante SQLAlchemy.                     │
  │  • Validación de suites con Great Expectations.                           │
  │  • Programación y control mediante DAGs de Apache Airflow.                │
  └───────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
  ┌───────────────────────────────────────────────────────────────────────────┐
  │ EDA Y VISUALIZACIÓN                                                       │
  │  • Consultas SQL con pd.read_sql() sobre el Esquema de Galaxia.           │
  │  • Agregaciones y métricas ponderadas por factor de expansión (fex_c/fexp).│
  └───────────────────────────────────────────────────────────────────────────┘
1. ¿Los hogares con mayor número de personas presentan mayor probabilidad de estar en pobreza extrema?
2. ¿En qué departamentos se concentran los hogares con menores ingresos?
3. ¿La pobreza extrema se distribuye igual entre zonas urbanas y rurales, y qué tan lejos están de salir de ella los hogares que ya están en indigencia?
Preguntas Nivel Persona:
4. ¿Las personas de hogares pobres tienen mayor presencia en trabajos de baja remuneración o en múltiples empleos?
5. ¿Dónde hay más pobreza extrema, a nivel de personas?
6. ¿Quiénes están en pobreza extrema (sexo, edad)?
7. ¿Qué relación tiene el nivel educativo con la pobreza extrema?
8. ¿Cómo se comportan los ingresos de las personas según la condición de pobreza de su hogar?
 Detalle de las Entregas
Entrega 1: Pobreza Monetaria y Extrema (GEIH - DANE)
Procesamiento de los microdatos oficiales de Hogares y Personas.
Refinamiento Medallón para Personas:
Bronce: Microdatos crudos del CSV.
Plata: Categorías legibles (sexo, grupo_edad, nivel_educativo) y consolidación de condicion_laboral a partir de oc, des y fft.
Oro: Variables analíticas optimizadas (log_ingreso, tiene_segundo_empleo) conectadas con dim_hogar.
Entrega 2: API de Pobreza Multidimensional (IPM)
Ingesta resiliente desde la API en Docker con requests.Session y reintentos.
Verificación de consistencia matemática del IPM:
• IPM = ∑ (Indicador_i × Peso_i)
• pobre = 1 ⟺ IPM ≥ 1/3
• fexp = fex_c × personas
Evaluación de los 15 indicadores en 5 dimensiones (Educación 20%, Niñez/Juventud 20%, Trabajo 20%, Salud 20%, Vivienda/Servicios 20%).
Generación de reportes EDA en reports/ y modularización para Apache Airflow y Great Expectations.
🗄️ Modelo Dimensional / Data Warehouse (Esquema de Galaxia)
Tablas de Hechos:
fact_hogares: Registro de hogares, ingresos acumulados y condición de pobreza extrema.
fact_personas: Registro individual de personas, condición laboral y nivel de ingreso individual.
fact_ipm: Puntuación del IPM, condición de pobreza multidimensional y los 15 indicadores de privación.
Dimensiones Compartidas:
dim_geografia: Códigos DANE de departamentos y nombres oficiales.
dim_tiempo: Fechas de corte y temporalidad del análisis.
dim_hogar: Identificador único del hogar (directorio-secuencia_p).
Dimensión Específica:
dim_persona: Atributos sociodemográficos de los individuos (sexo, grupo de edad, nivel educativo).
⚙️ Cómo Ejecutar el Proyecto
Requisitos Previos
Cuenta de Google para Google Colab o entorno local con Python 3.10+.
Proyecto de PostgreSQL en Supabase activo.
Archivos Hogares.csv y Personas.csv en la carpeta data/raw/ o en Google Drive.
Conexión a la red o servicio donde se aloja la API del IPM.
Pasos
1. Clonar el repositorio:
git clone <url-del-repositorio>
cd <nombre-del-repositorio>
2. Ejecutar Entrega 1:
• Abrir notebooks/ETL_EDA_Hogares_DEF.ipynb en Colab, conectar a Supabase e ingresar credenciales para crear e ingestar las tablas de Hogares y Geografía.
• Abrir notebooks/ETL_personas_DEF.ipynb para procesar la capa Medallón de Personas y cargar dim_persona y fact_personas.
3. Ejecutar Entrega 2:
• Abrir notebooks/ETL_IPM_API_DEF.ipynb para ejecutar la extracción desde la API Docker, la transformación en staging/, el EDA visual y la validación con Great Expectations.
📁 Estructura del Repositorio


.
├── README.md
├── .gitignore
├── data/
│   ├── raw/                      # Microdatos originales GEIH (Hogares.csv, Personas.csv)
│   ├── staging/                  # Archivos procesados intermedios (ipm_api_raw.csv)
│   └── Enlaces datasets.txt
├── notebooks/
│   ├── ETL_EDA_Hogares_DEF.ipynb # Notebook de Hogares (GEIH)
│   ├── ETL_personas_DEF.ipynb    # Notebook de Personas (GEIH - Medallón)
│   └── ETL_IPM_API_DEF.ipynb     # Notebook de API IPM Docker
├── dags/                         # DAGs de Apache Airflow
├── reports/                      # Visualizaciones y reportes de calidad
└── documentos/
    ├── ETL-Project_First Delivery.pdf
    └── ETL-Project_Second Delivery.pdf


