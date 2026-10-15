"""
Proyecto ETL - Segunda entrega - Pipeline con Apache Airflow
Fuentes: API (IPM), CSV (viviendas) y base de datos PostgreSQL (personas y pobreza monetaria)
Destinos: tabla merge_departamento (base relacional) y CSV con el mismo contenido
Calidad: Great Expectations dentro de las tareas transform_* y quality_check
"""
import os, io, json, contextlib, warnings
from datetime import datetime, timedelta
from decimal import Decimal
from urllib.parse import urlsplit, unquote

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import URL

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

warnings.filterwarnings("ignore")

# carpetas y direcciones; se cambian con variables de entorno
BASE = os.environ.get("ETL_BASE", "./etl_proyecto")
STAGING = os.environ.get("ETL_STAGING", f"{BASE}/staging")   # archivos temporales entre tareas
REPORTES = f"{BASE}/reports"
API_URL = os.environ.get("API_URL", "http://201.32.130.152/api/productos")
CSV_PATH = os.environ.get("CSV_PATH", f"{BASE}/viviendas_pdtos.csv")
DESTINO_URL = os.environ.get("DESTINO_URL", f"sqlite:///{os.path.abspath(BASE)}/pobreza.db")
TIMEOUT = 120
LOG_VALIDACION = f"{REPORTES}/validation_log.csv"

FINAL_CSV = f"{BASE}/merge_departamento.csv"
TABLA_FINAL = "merge_departamento"
SCHEDULE = os.environ.get("DAG_SCHEDULE", "@daily")          # "none" = solo ejecución manual


def _ensure_dirs():
    for d in (STAGING, REPORTES):
        os.makedirs(d, exist_ok=True)

DEPARTAMENTOS = {
    "05": "Antioquia", "08": "Atlántico", "11": "Bogotá D.C.", "13": "Bolívar",
    "15": "Boyacá", "17": "Caldas", "18": "Caquetá", "19": "Cauca", "20": "Cesar",
    "23": "Córdoba", "25": "Cundinamarca", "27": "Chocó", "41": "Huila",
    "44": "La Guajira", "47": "Magdalena", "50": "Meta", "52": "Nariño",
    "54": "Norte de Santander", "63": "Quindío", "66": "Risaralda",
    "68": "Santander", "70": "Sucre", "73": "Tolima", "76": "Valle del Cauca",
    "81": "Arauca", "85": "Casanare", "86": "Putumayo",
    "88": "San Andrés y Providencia", "91": "Amazonas", "94": "Guainía",
    "95": "Guaviare", "97": "Vaupés", "99": "Vichada",
}
CODIGOS = {nombre: codigo for codigo, nombre in DEPARTAMENTOS.items()}


def ruta(nombre):
    return f"{STAGING}/{nombre}"


def validar(df, nombre_suite, expectativas, etiqueta="dag"):
    import great_expectations as gx

    d = df.copy()
    # great expectations trabaja mejor con tipos normales que con los que admiten nulos
    for c in d.columns:
        if str(d[c].dtype) == "Int64":
            d[c] = d[c].astype("float64")
        elif str(d[c].dtype) in ("string", "str"):
            d[c] = d[c].astype(object)

    contexto = gx.get_context(mode="ephemeral")
    activo = contexto.data_sources.add_pandas("origen").add_dataframe_asset(nombre_suite)
    lote = activo.add_batch_definition_whole_dataframe("lote").get_batch(batch_parameters={"dataframe": d})

    suite = gx.ExpectationSuite(name=nombre_suite)
    for e in expectativas:
        suite.add_expectation(e)
    with contextlib.redirect_stderr(io.StringIO()):
        resultado = lote.validate(suite)

    ahora = datetime.now().isoformat(timespec="seconds")
    filas = []
    for r in resultado.results:
        kw = {k: v for k, v in r.expectation_config.kwargs.items() if k != "batch_id"}
        filas.append({
            "fecha": ahora, "suite": nombre_suite, "run": etiqueta,
            "regla": r.expectation_config.type, "parametros": json.dumps(kw, ensure_ascii=False, default=str),
            "ok": bool(r.success), "porcentaje_fuera": r.result.get("unexpected_percent"),
        })
    log = pd.DataFrame(filas)
    log.to_csv(LOG_VALIDACION, mode="a", header=not os.path.exists(LOG_VALIDACION), index=False)

    fallidas = log[~log["ok"]]
    resumen = {"suite": nombre_suite, "run": etiqueta, "passed": bool(resultado.success),
               "reglas_total": len(log),
               "fallidas": fallidas[["regla", "parametros", "porcentaje_fuera"]].to_dict("records")}
    with open(f"{REPORTES}/ge_{nombre_suite}_{etiqueta}.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False, default=str)

    print(f"[GE] {nombre_suite} -> {'PASS' if resumen['passed'] else 'FAIL'} "
          f"({len(fallidas)} de {len(log)} reglas fallidas)")
    for _, f_ in fallidas.iterrows():
        print("   x", f_["regla"], f_["parametros"], "->", f_["porcentaje_fuera"], "% fuera")
    return resumen


def exigir(resumen):
    # si la validación falla, la tarea falla y el dag se detiene
    if not resumen["passed"]:
        raise AirflowFailException(f"La validación {resumen['suite']} falló: {resumen['fallidas']}")

# ------------------------------------------------------------ fuente 1: api (ipm)
PESOS_IPM = {
    "logro_educativo": 0.10, "analfabetismo": 0.10,
    "inasistencia_escolar": 0.05, "rezago_escolar": 0.05,
    "atencion_integral": 0.05, "trabajo_infantil": 0.05,
    "desempleo_larga_duracion": 0.10, "empleo_formal": 0.10,
    "aseguramiento_salud": 0.10, "barreras_acceso_salud": 0.10,
    "acueducto": 0.04, "alcantarillado": 0.04, "pisos": 0.04,
    "paredes": 0.04, "hacinamiento": 0.04,
}
COLS_DESCARTAR = ["p1075", "p1077s21", "p1077s22", "p1077s23"]
COLS_NUM = (["directorio", "secuencia_p", "secuencia_encuesta", "p5010", "p8526", "p8530",
             "region", "departamento", "personas", "fex_c", "fexp", "ipm", "pobre", "periodo"]
            + list(PESOS_IPM) + COLS_DESCARTAR)


def _sesion():
    s = requests.Session()
    retry = Retry(total=4, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def _registros(payload):
    # devuelve (lista de registros, url siguiente o None)
    if isinstance(payload, list):
        return payload, None
    if isinstance(payload, dict):
        siguiente = payload.get("next") or payload.get("next_page")
        siguiente = siguiente if isinstance(siguiente, str) and siguiente.startswith("http") else None
        for k in ("data", "results", "items", "productos", "rows", "records"):
            if isinstance(payload.get(k), list):
                return payload[k], siguiente
        for v in payload.values():
            if isinstance(v, list):
                return v, siguiente
    raise ValueError(f"No se encontró la lista de registros en la respuesta: {str(payload)[:300]}")


def descargar_api(url=None, max_paginas=1000):
    url = url or API_URL
    s = _sesion()
    registros, paginas = [], 0
    while url and paginas < max_paginas:
        r = s.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        lote, url = _registros(r.json())
        registros.extend(lote)
        paginas += 1
    df = pd.DataFrame(registros)
    print(f"[read_api] {len(df):,} filas x {df.shape[1]} columnas en {paginas} petición(es)")
    return df


def normalizar(df):
    d = df.copy()
    d.columns = [c.strip().lower() for c in d.columns]
    d = d.replace({"NA": np.nan, "": np.nan})
    for c in COLS_NUM:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    return d


def chequeos_consistencia(d):
    ind = [c for c in PESOS_IPM if c in d.columns]
    ipm_rec = sum(d[c] * PESOS_IPM[c] for c in ind)
    return pd.DataFrame({
        "ipm_coincide": (d["ipm"] - ipm_rec).abs() < 0.005,
        "pobre_coincide": d["pobre"] == (ipm_rec >= 1 / 3 - 1e-9).astype(int),
        "fexp_coincide": np.isclose(d["fexp"], d["fex_c"] * d["personas"], rtol=1e-4),
    })


def limpiar_api(df_raw):
    df = normalizar(df_raw)

    antes = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    print(f"[transform_api] duplicados exactos eliminados: {antes - len(df):,}")

    # departamento: código de 2 dígitos y nombre
    dep = df["departamento"].astype("Int64")
    df["codigo_departamento"] = dep.astype("string").str.zfill(2)
    df["nombre_departamento"] = df["codigo_departamento"].map(DEPARTAMENTOS)
    sin_nombre = int(df["nombre_departamento"].isna().sum())
    if sin_nombre:
        print(f"[transform_api] ATENCIÓN: {sin_nombre:,} filas con departamento fuera del catálogo")

    # llave del hogar
    df["id_hogar"] = (df["directorio"].astype("Int64").astype("string") + "-"
                      + df["secuencia_p"].astype("Int64").astype("string"))

    # fecha de corte: el periodo viene lleno solo en parte de las filas
    if "periodo" in df.columns:
        texto = df["periodo"].astype("Int64").astype("string")
        fechas = pd.to_datetime(texto, format="%Y%m%d", errors="coerce")
        moda = fechas.dropna().mode()
        df["fecha_corte"] = moda.iloc[0] if len(moda) else pd.NaT
        df = df.drop(columns=["periodo"])

    ind = [c for c in PESOS_IPM if c in df.columns]
    df["n_privaciones"] = df[ind].sum(axis=1).astype(int)

    df = df.drop(columns=[c for c in COLS_DESCARTAR if c in df.columns])
    if "secuencia_encuesta" in df.columns and (df["secuencia_encuesta"] == df["personas"]).all():
        df = df.drop(columns=["secuencia_encuesta"])

    print(f"[transform_api] resultado: {df.shape[0]:,} filas x {df.shape[1]} columnas")
    return df


def preparar_validacion(df):
    chk = chequeos_consistencia(df).astype(int).add_prefix("chk_")
    return pd.concat([df, chk], axis=1)


def reglas_api(df):
    import great_expectations.expectations as gxe

    ind = [c for c in PESOS_IPM if c in df.columns]
    e = []
    for c in ["id_hogar", "directorio", "secuencia_p", "codigo_departamento", "nombre_departamento",
              "personas", "fex_c", "fexp", "ipm", "pobre", "fecha_corte"] + ind:
        e.append(gxe.ExpectColumnValuesToNotBeNull(column=c))
    for c in ind + ["pobre"]:
        e.append(gxe.ExpectColumnValuesToBeInSet(column=c, value_set=[0, 1]))
    e += [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToBeUnique(column="id_hogar"),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["directorio", "secuencia_p"]),
        gxe.ExpectColumnValuesToBeBetween(column="ipm", min_value=0, max_value=1),
        gxe.ExpectColumnValuesToBeBetween(column="personas", min_value=1, max_value=30),
        gxe.ExpectColumnValuesToBeBetween(column="n_privaciones", min_value=0, max_value=15),
        gxe.ExpectColumnValuesToBeBetween(column="fex_c", min_value=0, strict_min=True),
        gxe.ExpectColumnValuesToBeBetween(column="fexp", min_value=0, strict_min=True),
        gxe.ExpectColumnValuesToBeInSet(column="codigo_departamento", value_set=list(DEPARTAMENTOS)),
        gxe.ExpectColumnValuesToBeInSet(column="chk_pobre_coincide", value_set=[1]),
        gxe.ExpectColumnValuesToBeInSet(column="chk_ipm_coincide", value_set=[1], mostly=0.99),
        gxe.ExpectColumnValuesToBeInSet(column="chk_fexp_coincide", value_set=[1], mostly=0.99),
    ]
    return e

# ------------------------------------------------------------ fuente 2: csv (viviendas)
RENOMBRAR = {
    "p3": "clase",
    "p4005": "material_paredes",
    "p4015": "material_pisos",
    "p8520s3": "alcantarillado_vivienda",
    "p8520s5": "acueducto_vivienda",
}
ETIQUETAS = {
    "clase": {1: "Cabecera", 2: "Centro poblado", 3: "Rural disperso"},
    "material_paredes": {
        1: "Bloque, ladrillo, piedra, madera pulida", 2: "Tapia pisada, adobe",
        3: "Bahareque revocado", 4: "Bahareque sin revocar", 5: "Madera burda, tabla, tablón",
        6: "Material prefabricado", 7: "Guadua, caña, esterilla, otro vegetal",
        8: "Zinc, tela, carbón, latas, desechos, plástico", 9: "Sin paredes",
    },
    "material_pisos": {
        1: "Alfombra o tapete de pared a pared", 2: "Madera pulida y lacada, parqué", 3: "Mármol",
        4: "Baldosa, vinilo, tableta, ladrillo", 5: "Madera burda, tabla, tablón, otro vegetal",
        6: "Cemento, gravilla", 7: "Tierra, arena",
    },
    "alcantarillado_vivienda": {1: "Sí", 2: "No"},
    "acueducto_vivienda": {1: "Sí", 2: "No"},
}
VALORES_VALIDOS = {
    "clase": [1, 2, 3],
    "material_paredes": list(range(1, 10)),
    "material_pisos": list(range(1, 8)),
    "alcantarillado_vivienda": [1, 2],
    "acueducto_vivienda": [1, 2],
}


def leer_csv(path=None):
    path = path or CSV_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe {path}. Subir viviendas_pdtos.csv a esa ruta o cambiar CSV_PATH.")
    try:
        df = pd.read_csv(path, na_values=["NA", ""], low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(path, na_values=["NA", ""], low_memory=False, encoding="latin-1")
    print(f"[read_csv] {len(df):,} filas x {df.shape[1]} columnas")
    return df


def limpiar_csv(df_raw):
    df = df_raw.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    for c in df.columns:
        antes = df[c].notna().sum()
        num = pd.to_numeric(df[c], errors="coerce")
        perdidos = antes - num.notna().sum()
        if perdidos:
            print(f"[transform_csv] {c}: {perdidos:,} valores no numéricos pasaron a nulo")
        es_entero = (num.dropna() % 1 == 0).all()
        df[c] = num.astype("Int64") if es_entero else num

    n0 = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    print(f"[transform_csv] duplicados exactos eliminados: {n0 - len(df):,}")

    dep = df["departamento"].astype("Int64")
    df["codigo_departamento"] = dep.astype(str).str.zfill(2).where(dep.notna())
    df["nombre_departamento"] = df["codigo_departamento"].map(DEPARTAMENTOS)
    sin_nombre = df["nombre_departamento"].isna().sum()
    if sin_nombre:
        print(f"[transform_csv] ATENCIÓN: {sin_nombre:,} filas con departamento fuera del catálogo")

    llave = [c for c in ("directorio", "secuencia_p") if c in df.columns]
    if len(llave) == 2:
        rep = df.duplicated(subset=llave, keep=False).sum()
        if rep:
            print(f"[transform_csv] {rep:,} filas comparten la llave {llave} con valores distintos")
        df["id_hogar"] = df["directorio"].astype(str) + "-" + df["secuencia_p"].astype(str)

    df = df.rename(columns=RENOMBRAR)
    for col, mapa in ETIQUETAS.items():
        if col in df.columns:
            df[col + "_desc"] = df[col].map(mapa)

    print(f"[transform_csv] resultado: {df.shape[0]:,} filas x {df.shape[1]} columnas")
    return df


def reglas_csv(df):
    import great_expectations.expectations as gxe

    e = [gxe.ExpectTableRowCountToBeBetween(min_value=1)]
    for c in ("directorio", "secuencia_p", "id_hogar"):
        if c in df.columns:
            e.append(gxe.ExpectColumnValuesToNotBeNull(column=c))
    if "directorio" in df.columns:
        e.append(gxe.ExpectColumnValuesToBeBetween(column="directorio", min_value=1))
    if {"directorio", "secuencia_p"} <= set(df.columns):
        e.append(gxe.ExpectCompoundColumnsToBeUnique(column_list=["directorio", "secuencia_p"]))
    if "secuencia_p" in df.columns:
        e.append(gxe.ExpectColumnValuesToBeBetween(column="secuencia_p", min_value=1, max_value=6))
    if "secuencia_encuesta" in df.columns:
        e.append(gxe.ExpectColumnValuesToBeBetween(column="secuencia_encuesta", min_value=1, max_value=21))
    if "codigo_departamento" in df.columns:
        e.append(gxe.ExpectColumnValuesToBeInSet(column="codigo_departamento", value_set=list(DEPARTAMENTOS)))
    otras = [c for c in df.columns
             if c not in ("directorio", "secuencia_p", "secuencia_encuesta", "id_hogar",
                          "codigo_departamento", "nombre_departamento")
             and not c.endswith("_desc")]
    for c in otras:
        e.append(gxe.ExpectColumnValuesToNotBeNull(column=c, mostly=0.95))
        if c in VALORES_VALIDOS:
            e.append(gxe.ExpectColumnValuesToBeInSet(column=c, value_set=VALORES_VALIDOS[c], mostly=0.99))
        else:
            e.append(gxe.ExpectColumnValuesToBeBetween(column=c, min_value=0))
    return e

# ------------------------------------------------------------ fuente 3: base de datos
MAPA_EDUCACION = {
    "Ninguno": "Sin educación formal", "Preescolar": "Sin educación formal",
    "Básica primaria (1o - 5o)": "Básica", "Básica secundaria (6o - 9o)": "Básica",
    "Media académica (Bachillerato clásico)": "Media", "Media técnica (Bachillerato técnico)": "Media",
    "Normalista": "Media", "Técnica profesional": "Superior", "Tecnológica": "Superior",
    "Universitaria": "Superior", "Especialización": "Superior", "Maestría": "Superior",
    "Doctorado": "Superior",
}
COLS_TEXTO = ["sexo", "grupo_edad", "nivel_educativo", "condicion_laboral"]
USAR_COPIA_DB = True   # True: reutiliza los archivos ya guardados. False: consulta la base


def url_desde_cadena(cadena):
    partes = urlsplit(cadena.strip())
    if partes.scheme not in ("postgres", "postgresql") or not partes.hostname:
        raise ValueError("La cadena debe tener la forma postgresql://usuario:contraseña@host:puerto/base")
    return URL.create(
        "postgresql+psycopg2",
        username=unquote(partes.username or ""),
        password=unquote(partes.password or ""),
        host=partes.hostname,
        port=partes.port or 5432,
        database=partes.path.lstrip("/") or "postgres",
    )


def diagnostico(error):
    m = str(error).lower()
    if "unreachable" in m or "cannot assign requested address" in m:
        return "la conexión directa de Supabase usa IPv6 y Colab no. Usar la cadena del Session pooler."
    if "eauthquery" in m or "connection to database not available" in m:
        return "el pooler no pudo llegar a la base. Revisar que el proyecto de Supabase esté activo y esperar unos minutos."
    if "tenant or user not found" in m:
        return "con el pooler el usuario lleva el identificador del proyecto: usuario.identificador_del_proyecto."
    if "password authentication failed" in m:
        return "usuario o contraseña incorrectos."
    if "timeout" in m or "timed out" in m:
        return "no hubo respuesta. Revisar que el proyecto de Supabase no esté pausado."
    if "could not translate host" in m or "name or service not known" in m:
        return "el host está mal escrito."
    return "revisar la cadena de conexión y que el proyecto de Supabase esté activo."


def conectar():
    cadena = os.environ.get("DATABASE_URL")
    if not cadena:
        raise RuntimeError("Falta la variable DATABASE_URL con la cadena de conexión de la base de datos.")
    if not cadena.startswith(("postgres://", "postgresql://")):
        return create_engine(cadena)   # otro tipo de base (por ejemplo sqlite en pruebas)
    url = url_desde_cadena(cadena)
    intentos = [url]
    if "pooler.supabase.com" in (url.host or "") and url.port == 5432:
        intentos.append(url.set(port=6543))
    ultimo = None
    for u in intentos:
        try:
            motor = create_engine(u, pool_pre_ping=True, connect_args={"sslmode": "require", "connect_timeout": 20})
            with motor.connect() as con:
                con.execute(text("SELECT 1"))
            print(f"[conectar] conexión correcta a {u.host}:{u.port}")
            return motor
        except Exception as error:
            ultimo = error
            print("[conectar] no se pudo conectar:", str(error).strip().splitlines()[0][:200])
    raise RuntimeError("No hay conexión a la base de datos. Causa probable: " + diagnostico(ultimo)) from None


def consulta(motor, sql, filas_por_bloque=None):
    with motor.connect() as con:
        if motor.dialect.name == "postgresql":
            con.execute(text("SET statement_timeout = '10min'"))
        if filas_por_bloque is None:
            res = con.execute(text(sql))
            return pd.DataFrame.from_records([tuple(f) for f in res.fetchall()], columns=list(res.keys()), coerce_float=True)

        # Lectura por bloques con cursor en el servidor
        res = con.execution_options(stream_results=True).execute(text(sql))
        columnas = list(res.keys())
        bloques, total = [], 0
        while True:
            filas = res.fetchmany(filas_por_bloque)
            if not filas:
                break
            bloques.append(pd.DataFrame.from_records([tuple(f) for f in filas], columns=columnas, coerce_float=True))
            total += len(filas)
            print(f"  {total:,} filas leídas", flush=True)
        return pd.concat(bloques, ignore_index=True) if bloques else pd.DataFrame(columns=columnas)


def leer_personas(motor):
    tablas = inspect(motor).get_table_names()
    for t in ("fact_personas", "dim_persona"):
        if t not in tablas:
            raise RuntimeError(f"No existe la tabla {t}. Tablas disponibles: {tablas}")

    print("[read_db] leyendo personas...", flush=True)
    personas = consulta(motor, '''
        SELECT * FROM "fact_personas" fp
        JOIN "dim_persona" dp ON dp.id_persona = fp.id_persona
    ''', filas_por_bloque=50_000)
    personas = personas.loc[:, ~personas.columns.duplicated()]   # id_persona llega repetido por el join

    if "dim_geografia" in tablas:
        geo = consulta(motor, 'SELECT * FROM "dim_geografia"')
        nuevas = [c for c in geo.columns if c not in personas.columns or c == "id_geografia"]
        personas = personas.merge(geo[nuevas], on="id_geografia", how="left")
        print(f"[read_db] dim_geografia unida ({len(geo):,} filas)", flush=True)

    print(f"[read_db] personas: {len(personas):,} filas x {personas.shape[1]} columnas", flush=True)
    return personas


def leer_hogares(motor):
    print("[read_db] leyendo hogares...", flush=True)
    pobreza = consulta(motor, '''
        SELECT fh.fex_c, fh.pobre, dg.dpto AS departamento
        FROM "fact_hogares" fh
        LEFT JOIN "dim_geografia" dg ON fh.id_geografia = dg.id_geografia
    ''', filas_por_bloque=50_000)
    print(f"[read_db] hogares: {len(pobreza):,} filas", flush=True)
    return pobreza

def limpiar_db(df_raw):
    df = df_raw.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    # decimal de postgres -> número normal
    for c in df.columns:
        if df[c].dtype == object:
            muestra = df[c].dropna().head(50)
            if len(muestra) and all(isinstance(v, (int, float, Decimal)) for v in muestra):
                df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in COLS_TEXTO:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    n0 = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    print(f"[transform_db] duplicados exactos eliminados: {n0 - len(df):,}")

    for c in ("directorio", "secuencia_p"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    if "id_hogar" in df.columns:
        df = df.rename(columns={"id_hogar": "id_hogar_bd"})
    df["id_hogar"] = df["directorio"].astype("string") + "-" + df["secuencia_p"].astype("string")

    if "nivel_educativo" in df.columns:
        df["grupo_educativo"] = df["nivel_educativo"].map(MAPA_EDUCACION).fillna("Sin información")
    if "condicion_laboral" in df.columns:
        df["ocupado"] = (df["condicion_laboral"] == "Ocupado").fillna(False).astype(int)

    print(f"[transform_db] resultado: {df.shape[0]:,} filas x {df.shape[1]} columnas")
    return df


def resumen_hogar(df):
    ocupado = df["ocupado"] if "ocupado" in df.columns else 0
    out = (df.assign(_ocupado=ocupado)
             .groupby(["directorio", "secuencia_p"], dropna=False)
             .agg(personas_db=("id_persona", "count"),
                  ingreso_total_hogar=("ingtot", "sum"),
                  ingreso_prom_persona=("ingtot", "mean"),
                  personas_ocupadas=("_ocupado", "sum"))
             .reset_index())
    out["id_hogar"] = out["directorio"].astype("string") + "-" + out["secuencia_p"].astype("string")
    print(f"[resumen_hogar] {len(out):,} hogares")
    return out


def resumen_monetaria(pobreza):
    # pobreza monetaria por departamento, ponderada con fex_c
    h = pobreza.assign(pobre=pobreza["pobre"].astype("float64"), fex_c=pobreza["fex_c"].astype("float64"))
    h["_pobre"] = h["pobre"] * h["fex_c"]
    out = (h.groupby("departamento")
            .agg(hogares_db=("pobre", "size"), suma_pobre=("_pobre", "sum"), suma_peso=("fex_c", "sum"))
            .reset_index())
    out["pobreza_monetaria"] = out["suma_pobre"] / out["suma_peso"] * 100
    print(f"[resumen_monetaria] {len(out)} departamentos")
    return out[["departamento", "hogares_db", "pobreza_monetaria"]]


def reglas_personas(df):
    import great_expectations.expectations as gxe

    e, cols = [], df.columns
    for c in ("id_persona", "id_hogar", "directorio", "secuencia_p", "fex_c"):
        if c in cols:
            e.append(gxe.ExpectColumnValuesToNotBeNull(column=c))
    e.append(gxe.ExpectColumnValuesToBeUnique(column="id_persona"))
    e.append(gxe.ExpectTableRowCountToBeBetween(min_value=1))
    if "fex_c" in cols:
        e.append(gxe.ExpectColumnValuesToBeBetween(column="fex_c", min_value=0, strict_min=True))
    if "p6040" in cols:
        e.append(gxe.ExpectColumnValuesToBeBetween(column="p6040", min_value=0, max_value=110, mostly=0.99))
    if "ingtot" in cols:
        e.append(gxe.ExpectColumnValuesToBeBetween(column="ingtot", min_value=0, mostly=0.99))
    if "sexo" in cols:
        e.append(gxe.ExpectColumnValuesToBeInSet(column="sexo", value_set=["Hombre", "Mujer"], mostly=0.99))
    if "condicion_laboral" in cols:
        e.append(gxe.ExpectColumnValuesToBeInSet(
            column="condicion_laboral",
            value_set=["Ocupado", "Desocupado", "Fuera de la fuerza de trabajo"], mostly=0.95))
    return e


def reglas_hogares(df):
    import great_expectations.expectations as gxe

    return [
        gxe.ExpectColumnValuesToNotBeNull(column="directorio"),
        gxe.ExpectColumnValuesToNotBeNull(column="secuencia_p"),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["directorio", "secuencia_p"]),
        gxe.ExpectColumnValuesToBeBetween(column="personas_db", min_value=1),
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
    ]

# ------------------------------------------------------------ merge por departamento
def resumir_ipm(ipm):
    d = ipm.assign(_pobre=ipm["pobre"] * ipm["fex_c"], _ipm=ipm["ipm"] * ipm["fex_c"])
    out = d.groupby("codigo_departamento").agg(
        hogares_ipm=("id_hogar", "count"),
        suma_pobre=("_pobre", "sum"),
        suma_ipm=("_ipm", "sum"),
        suma_peso=("fex_c", "sum"),
    )
    out["incidencia_ipm"] = out["suma_pobre"] / out["suma_peso"] * 100
    out["ipm_promedio"] = out["suma_ipm"] / out["suma_peso"]
    print(f"[resumir_ipm] {len(out)} departamentos")
    return out[["hogares_ipm", "incidencia_ipm", "ipm_promedio"]].reset_index()


def resumir_csv(csv):
    d = pd.DataFrame({
        "codigo_departamento": csv["codigo_departamento"],
        "id_hogar": csv["id_hogar"],
        "sin_acueducto": (csv["acueducto_vivienda"] == 2).astype("float64"),
        "sin_alcantarillado": (csv["alcantarillado_vivienda"] == 2).astype("float64"),
        "piso_tierra": (csv["material_pisos"] == 7).astype("float64"),
        "rural_disperso": (csv["clase"] == 3).astype("float64"),
    })
    out = d.groupby("codigo_departamento").agg(
        hogares_csv=("id_hogar", "count"),
        pct_sin_acueducto=("sin_acueducto", "mean"),
        pct_sin_alcantarillado=("sin_alcantarillado", "mean"),
        pct_piso_tierra=("piso_tierra", "mean"),
        pct_rural_disperso=("rural_disperso", "mean"),
    )
    pct = [c for c in out.columns if c.startswith("pct_")]
    out[pct] = out[pct] * 100
    print(f"[resumir_csv] {len(out)} departamentos")
    return out.reset_index()


def resumir_db(db):
    out = db.copy()
    out["departamento"] = out["departamento"].astype("string").str.strip()
    out["codigo_departamento"] = out["departamento"].map(CODIGOS)
    sin_codigo = out.loc[out["codigo_departamento"].isna(), "departamento"].tolist()
    if sin_codigo:
        print(f"[resumir_db] ATENCIÓN: departamentos sin código en el catálogo: {sin_codigo}")
    out = out.dropna(subset=["codigo_departamento"])
    print(f"[resumir_db] {len(out)} departamentos")
    return out[["codigo_departamento", "hogares_db", "pobreza_monetaria"]].reset_index(drop=True)


def merge_departamento(res_ipm, res_csv, res_db):
    m = res_ipm.merge(res_csv, on="codigo_departamento", how="outer")
    m = m.merge(res_db, on="codigo_departamento", how="outer")

    m["nombre_departamento"] = m["codigo_departamento"].map(DEPARTAMENTOS)
    m["en_ipm"] = m["hogares_ipm"].notna()
    m["en_csv"] = m["hogares_csv"].notna()
    m["en_db"] = m["hogares_db"].notna()
    m["diferencia_ipm_monetaria"] = m["incidencia_ipm"] - m["pobreza_monetaria"]

    orden = ["codigo_departamento", "nombre_departamento", "en_ipm", "en_csv", "en_db",
             "hogares_ipm", "incidencia_ipm", "ipm_promedio",
             "hogares_csv", "pct_sin_acueducto", "pct_sin_alcantarillado",
             "pct_piso_tierra", "pct_rural_disperso",
             "hogares_db", "pobreza_monetaria", "diferencia_ipm_monetaria"]
    m = m[orden].sort_values("codigo_departamento").reset_index(drop=True)
    print(f"[merge_departamento] resultado: {m.shape[0]} filas x {m.shape[1]} columnas")
    return m


def reglas_merge(df):
    import great_expectations.expectations as gxe

    e = [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="codigo_departamento"),
        gxe.ExpectColumnValuesToBeUnique(column="codigo_departamento"),
        gxe.ExpectColumnValuesToBeInSet(column="codigo_departamento", value_set=list(DEPARTAMENTOS)),
        gxe.ExpectColumnValuesToNotBeNull(column="nombre_departamento"),
    ]
    for c in ("hogares_ipm", "incidencia_ipm", "ipm_promedio", "hogares_csv",
              "pct_sin_acueducto", "pct_sin_alcantarillado", "pct_piso_tierra", "pct_rural_disperso"):
        e.append(gxe.ExpectColumnValuesToNotBeNull(column=c))
    e.append(gxe.ExpectColumnValuesToNotBeNull(column="pobreza_monetaria", mostly=0.7))
    for c in ("incidencia_ipm", "pobreza_monetaria", "pct_sin_acueducto", "pct_sin_alcantarillado",
              "pct_piso_tierra", "pct_rural_disperso"):
        e.append(gxe.ExpectColumnValuesToBeBetween(column=c, min_value=0, max_value=100))
    e.append(gxe.ExpectColumnValuesToBeBetween(column="ipm_promedio", min_value=0, max_value=1))
    for c in ("hogares_ipm", "hogares_csv", "hogares_db"):
        e.append(gxe.ExpectColumnValuesToBeBetween(column=c, min_value=1))
    return e


# ------------------------------------------------------------ tareas del dag
# cada tarea guarda su resultado en la carpeta de temporales y la siguiente lo lee
def read_api():
    """Descarga el IPM de la API (con paginación y reintentos) y guarda una copia temporal."""
    _ensure_dirs()
    df = descargar_api()
    df.to_pickle(ruta("ipm_api_raw.pkl"))
    return int(len(df))


def read_csv():
    """Lee el csv de viviendas y guarda una copia temporal."""
    _ensure_dirs()
    df = leer_csv()
    df.to_parquet(ruta("csv_raw.parquet"), index=False)
    return int(len(df))


def read_db():
    """Lee personas y hogares de la base de datos (o de la copia guardada) y los deja en staging."""
    _ensure_dirs()
    motor = None

    if USAR_COPIA_DB and os.path.exists(ruta("db_raw.parquet")):
        print("[read_db] se usa la copia guardada de personas (db_raw.parquet)", flush=True)
        personas = pd.read_parquet(ruta("db_raw.parquet"))
    else:
        motor = conectar()
        personas = leer_personas(motor)
        personas.to_parquet(ruta("db_raw.parquet"), index=False)

    if USAR_COPIA_DB and os.path.exists(ruta("db_pobreza_raw.parquet")):
        print("[read_db] se usa la copia guardada de hogares (db_pobreza_raw.parquet)", flush=True)
    else:
        motor = motor or conectar()
        pobreza = leer_hogares(motor)
        pobreza.to_parquet(ruta("db_pobreza_raw.parquet"), index=False)

    return int(len(personas))


def transform_api():
    """Limpia el IPM y lo valida con Great Expectations. Si no cumple las reglas, se detiene."""
    df = limpiar_api(pd.read_pickle(ruta("ipm_api_raw.pkl")))
    dv = preparar_validacion(df)
    exigir(validar(dv, "suite_api_ipm", reglas_api(dv)))
    df.to_parquet(ruta("ipm_api_clean.parquet"), index=False)
    return int(len(df))


def transform_csv():
    """Limpia el csv de viviendas y lo valida con Great Expectations. Si no cumple las reglas, se detiene."""
    df = limpiar_csv(pd.read_parquet(ruta("csv_raw.parquet")))
    exigir(validar(df, "suite_csv", reglas_csv(df)))
    df.to_parquet(ruta("csv_clean.parquet"), index=False)
    return int(len(df))


def transform_db():
    """Limpia personas, resume hogares y pobreza monetaria, y valida con Great Expectations."""
    personas = limpiar_db(pd.read_parquet(ruta("db_raw.parquet")))
    hogares = resumen_hogar(personas)
    monetaria = resumen_monetaria(pd.read_parquet(ruta("db_pobreza_raw.parquet")))
    exigir(validar(personas, "suite_db_personas", reglas_personas(personas)))
    exigir(validar(hogares, "suite_db_hogares", reglas_hogares(hogares)))
    personas.to_parquet(ruta("db_personas_clean.parquet"), index=False)
    hogares.to_parquet(ruta("db_hogares_clean.parquet"), index=False)
    monetaria.to_parquet(ruta("db_monetaria_dpto.parquet"), index=False)
    return int(len(personas))


def merge():
    """
    Resume cada fuente a una fila por departamento y las une por codigo_departamento.
    Se conservan todos los departamentos de cualquier fuente (outer join) y se marca de cuál viene cada uno.
    """
    ipm = pd.read_parquet(ruta("ipm_api_clean.parquet"))
    csv = pd.read_parquet(ruta("csv_clean.parquet"))
    db = pd.read_parquet(ruta("db_monetaria_dpto.parquet"))
    m = merge_departamento(resumir_ipm(ipm), resumir_csv(csv), resumir_db(db))

    # revisión: no se debe repetir ningún departamento
    if m["codigo_departamento"].duplicated().any():
        raise AirflowFailException("El merge repitió departamentos.")
    m.to_parquet(ruta("merge_departamento_sin_validar.parquet"), index=False)
    return int(len(m))


def quality_check():
    """Valida el resultado del merge con Great Expectations. Si falla, no se carga nada."""
    m = pd.read_parquet(ruta("merge_departamento_sin_validar.parquet"))
    exigir(validar(m, "suite_merge_departamento", reglas_merge(m)))
    m.to_parquet(ruta("merge_departamento.parquet"), index=False)
    return int(len(m))


def load():
    """Guarda en la base relacional la dimensión de departamentos y la tabla de hechos, y revisa las filas."""
    m = pd.read_parquet(ruta("merge_departamento.parquet"))
    motor = create_engine(DESTINO_URL)

    # dimensión: un nombre por código de departamento
    dim = (m[["codigo_departamento", "nombre_departamento"]]
           .drop_duplicates().sort_values("codigo_departamento"))
    if dim["codigo_departamento"].duplicated().any():
        raise AirflowFailException("Un código de departamento tiene más de un nombre.")

    dim.to_sql("dim_departamento", motor, if_exists="replace", index=False)
    m.to_sql(TABLA_FINAL, motor, if_exists="replace", index=False)

    n_bd = int(pd.read_sql_query(f"SELECT COUNT(*) AS n FROM {TABLA_FINAL}", motor)["n"].iloc[0])
    n_dim = int(pd.read_sql_query("SELECT COUNT(*) AS n FROM dim_departamento", motor)["n"].iloc[0])
    if n_bd != len(m) or n_dim != len(dim):
        raise AirflowFailException(f"Conteo distinto tras la carga: hechos {n_bd}/{len(m)}, dimensión {n_dim}/{len(dim)}")
    print(f"[load] dim_departamento: {n_dim} filas | {TABLA_FINAL}: {n_bd} filas en {DESTINO_URL.split('@')[-1]}")
    return n_bd

def store():
    """Genera el csv final leyendo la tabla de la base de datos, así que es igual a lo que hay en la base."""
    df = pd.read_sql_query(f"SELECT * FROM {TABLA_FINAL}", create_engine(DESTINO_URL))
    df.to_csv(FINAL_CSV, index=False)
    print(f"[store] CSV final: {FINAL_CSV} ({len(df)} filas)")
    return FINAL_CSV


# dag
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,                       # un reintento por si el fallo es pasajero
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="pobreza_dag",
    description="API (IPM) + CSV (viviendas) + base de datos -> merge por departamento -> calidad (Great Expectations) -> base relacional + CSV",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None if SCHEDULE.lower() == "none" else SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "pobreza", "great_expectations"],
) as dag:

    t_read_api = PythonOperator(task_id="read_api", python_callable=read_api)
    t_transform_api = PythonOperator(task_id="transform_api", python_callable=transform_api, retries=0)

    t_read_csv = PythonOperator(task_id="read_csv", python_callable=read_csv)
    t_transform_csv = PythonOperator(task_id="transform_csv", python_callable=transform_csv, retries=0)

    t_read_db = PythonOperator(task_id="read_db", python_callable=read_db,retries=0, execution_timeout=timedelta(minutes=25),)
    t_transform_db = PythonOperator(task_id="transform_db", python_callable=transform_db, retries=0)

    t_merge = PythonOperator(task_id="merge", python_callable=merge, retries=0)
    t_quality_check = PythonOperator(task_id="quality_check", python_callable=quality_check, retries=0)
    t_load = PythonOperator(task_id="load", python_callable=load)
    t_store = PythonOperator(task_id="store", python_callable=store)

    t_read_api >> t_transform_api >> t_merge
    t_read_csv >> t_transform_csv >> t_merge
    t_read_db >> t_transform_db >> t_merge
    t_merge >> t_quality_check >> t_load >> t_store
