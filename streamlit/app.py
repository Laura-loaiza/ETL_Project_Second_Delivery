import os
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# 1. Configuración y carga de datos

st.set_page_config(
    page_title="Pobreza y Vivienda en Colombia",
    page_icon="🏡",
    layout="wide"
)

st.title("🏡 Pobreza y condiciones de vivienda en Colombia")
st.write(
    "Explora los indicadores departamentales de pobreza monetaria, "
    "pobreza multidimensional y condiciones de vivienda."
)

@st.cache_data(ttl=600)   # se vuelve a leer cada 10 minutos
def cargar_datos():
    # primero el data warehouse; si no hay credencial o falla, el csv que exporta el DAG
    destino = os.environ.get("DESTINO_URL")
    if destino:
        try:
            motor = create_engine(destino)
            df = pd.read_sql_query("SELECT * FROM merge_departamento ORDER BY codigo_departamento", motor)
            return df, "Data Warehouse (PostgreSQL)"
        except Exception as error:
            motivo = f"no se pudo leer la base: {type(error).__name__}"
    else:
        motivo = "sin DESTINO_URL"
    candidatas = [
        os.environ.get("MERGE_CSV"),
        "/content/drive/My Drive/Colab Notebooks/etl_proyecto/merge_departamento.csv",
        "/content/drive/MyDrive/Colab Notebooks/etl_proyecto/merge_departamento.csv",
        "merge_departamento.csv",
    ]
    ruta = next((r for r in candidatas if r and os.path.exists(r)), None)
    if ruta is None:
        raise FileNotFoundError(
            "No se encontró merge_departamento.csv. Montar Drive (celda 2) y revisar que esté en "
            "Colab Notebooks/etl_proyecto. Rutas probadas: " + ", ".join(r for r in candidatas if r))
    df = pd.read_csv(ruta, dtype={"codigo_departamento": str})   # str para conservar el 0 de "05"
    return df, f"CSV exportado del Data Warehouse ({motivo})"

df, origen_datos = cargar_datos()
st.sidebar.caption(f"Fuente de los datos: {origen_datos}")

# Asegurar que las columnas numéricas sean numéricas
columnas_numericas = [
    "pobreza_monetaria",
    "incidencia_ipm",
    "ipm_promedio",
    "pct_sin_acueducto",
    "pct_sin_alcantarillado",
    "pct_piso_tierra",
    "pct_rural_disperso",
    "hogares_ipm",
    "hogares_csv",
    "hogares_db"
]

for col in columnas_numericas:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")


# 2. Filtros

departamentos = sorted(
    df["nombre_departamento"].dropna().unique()
)

seleccion = st.sidebar.selectbox(
    "Selecciona un departamento",
    ["Todos los departamentos"] + departamentos
)

if seleccion == "Todos los departamentos":
    datos = df.copy()
else:
    datos = df[df["nombre_departamento"] == seleccion].copy()

st.sidebar.markdown("---")
st.sidebar.write("**Fuentes de información**")
st.sidebar.caption("IPM de hogares, viviendas y pobreza monetaria.")


# 3. Indicadores generales

st.header("1. Resumen de indicadores")

if seleccion == "Todos los departamentos":
    st.caption(
        "Los valores mostrados son promedios simples entre departamentos "
        "con datos disponibles, no promedios nacionales ponderados."
    )

fila = datos.iloc[0] if seleccion != "Todos los departamentos" else None

def promedio(columna):
    if columna not in datos.columns:
        return None
    valores = datos[columna].dropna()
    return valores.mean() if not valores.empty else None

def mostrar_porcentaje(valor):
    return "Sin datos" if pd.isna(valor) else f"{valor:.2f}%"

def mostrar_numero(valor):
    return "Sin datos" if pd.isna(valor) else f"{valor:,.2f}"

if seleccion == "Todos los departamentos":
    pobreza = promedio("pobreza_monetaria")
    incidencia = promedio("incidencia_ipm")
    ipm = promedio("ipm_promedio")
    acueducto = promedio("pct_sin_acueducto")
    sin_alcantarillado = promedio("pct_sin_alcantarillado")
    piso_tierra = promedio("pct_piso_tierra")
    rural = promedio("pct_rural_disperso")
else:
    pobreza = fila.get("pobreza_monetaria")
    incidencia = fila.get("incidencia_ipm")
    ipm = fila.get("ipm_promedio")
    acueducto = fila.get("pct_sin_acueducto")
    sin_alcantarillado = fila.get("pct_sin_alcantarillado")
    piso_tierra = fila.get("pct_piso_tierra")
    rural = fila.get("pct_rural_disperso")

c1, c2, c3 = st.columns(3)
c1.metric("Pobreza monetaria", mostrar_porcentaje(pobreza))
c2.metric("Incidencia del IPM", mostrar_porcentaje(incidencia))
c3.metric("IPM promedio", mostrar_numero(ipm))

c4, c5, c6, c7 = st.columns(4)
c4.metric("Sin acueducto", mostrar_porcentaje(acueducto))
c5.metric("Sin alcantarillado", mostrar_porcentaje(sin_alcantarillado))
c6.metric("Piso de tierra", mostrar_porcentaje(piso_tierra))
c7.metric("Rural disperso", mostrar_porcentaje(rural))

# 4. Comparación de pobreza e IPM

st.header("2. Pobreza monetaria e IPM")

if seleccion == "Todos los departamentos":
    comparacion = datos.dropna(
        subset=["pobreza_monetaria", "ipm_promedio"]
    )

    if not comparacion.empty:
        fig = px.scatter(
            comparacion,
            x="pobreza_monetaria",
            y="ipm_promedio",
            hover_name="nombre_departamento",
            color="incidencia_ipm",
            size="incidencia_ipm",
            labels={
                "pobreza_monetaria": "Pobreza monetaria (%)",
                "ipm_promedio": "IPM promedio",
                "incidencia_ipm": "Incidencia IPM (%)"
            },
            title="Pobreza monetaria frente al IPM promedio"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay suficientes datos para esta comparación.")
else:
    st.info(
        "Selecciona «Todos los departamentos» para comparar "
        "los departamentos en el gráfico."
    )


# 5. Condiciones de vivienda

st.header("3. Condiciones de vivienda")

columnas_vivienda = {
    "pct_sin_acueducto": "Sin acueducto",
    "pct_sin_alcantarillado": "Sin alcantarillado",
    "pct_piso_tierra": "Piso de tierra",
    "pct_rural_disperso": "Rural disperso"
}

disponibles = {
    col: nombre
    for col, nombre in columnas_vivienda.items()
    if col in datos.columns and datos[col].notna().any()
}

if disponibles:
    if seleccion == "Todos los departamentos":
        vivienda = datos[
            ["nombre_departamento"] + list(disponibles.keys())
        ].melt(
            id_vars="nombre_departamento",
            var_name="indicador",
            value_name="porcentaje"
        )

        vivienda["indicador"] = vivienda["indicador"].map(disponibles)
        vivienda = vivienda.dropna(subset=["porcentaje"])

        fig_vivienda = px.bar(
            vivienda,
            x="nombre_departamento",
            y="porcentaje",
            color="indicador",
            barmode="group",
            labels={
                "nombre_departamento": "Departamento",
                "porcentaje": "Porcentaje de registros (%)",
                "indicador": "Condición"
            },
            title="Condiciones de vivienda por departamento"
        )
        fig_vivienda.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_vivienda, use_container_width=True)

    else:
        vivienda = pd.DataFrame({
            "Condición": [
                disponibles[col] for col in disponibles
            ],
            "Porcentaje": [
                fila[col] for col in disponibles
            ]
        }).dropna(subset=["Porcentaje"])

        fig_vivienda = px.bar(
            vivienda,
            x="Porcentaje",
            y="Condición",
            orientation="h",
            text="Porcentaje",
            labels={"Porcentaje": "Porcentaje de registros (%)"},
            title=f"Condiciones de vivienda: {seleccion}"
        )
        fig_vivienda.update_traces(texttemplate="%{text:.2f}%")
        st.plotly_chart(fig_vivienda, use_container_width=True)
else:
    st.warning("No hay datos disponibles de condiciones de vivienda.")

st.caption(
    "Importante: los porcentajes de vivienda son descriptivos de los "
    "registros observados, pues no se utilizó un factor de expansión. "
    "No deben interpretarse automáticamente como estimaciones "
    "representativas de toda la población."
)


# 6. Disponibilidad de las fuentes

st.header("4. Disponibilidad de información")

fuentes = [
    ("en_ipm", "IPM hogares"),
    ("en_csv", "Viviendas"),
    ("en_db", "Pobreza monetaria")
]

fuentes_disponibles = [
    (col, nombre) for col, nombre in fuentes if col in datos.columns
]

if fuentes_disponibles:
    cobertura = datos[
        ["nombre_departamento"] +
        [col for col, _ in fuentes_disponibles]
    ].melt(
        id_vars="nombre_departamento",
        var_name="fuente",
        value_name="disponible"
    )

    nombres_fuentes = dict(fuentes_disponibles)
    cobertura["fuente"] = cobertura["fuente"].map(nombres_fuentes)
    cobertura["disponible"] = (
        cobertura["disponible"].fillna(False).astype(bool)
    )

    fig_cobertura = px.bar(
        cobertura,
        x="nombre_departamento",
        color="disponible",
        facet_row="fuente",
        barmode="group",
        labels={
            "nombre_departamento": "Departamento",
            "disponible": "Datos disponibles",
            "fuente": "Fuente"
        },
        title="Disponibilidad de datos por fuente"
    )
    fig_cobertura.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig_cobertura, use_container_width=True)


# 7. Tabla departamental completa

st.header("5. Tabla departamental completa")

st.dataframe(
    datos,
    use_container_width=True,
    hide_index=True
)

st.download_button(
    label="Descargar datos filtrados en CSV",
    data=datos.to_csv(index=False).encode("utf-8-sig"),
    file_name="datos_departamentales.csv",
    mime="text/csv"
)
