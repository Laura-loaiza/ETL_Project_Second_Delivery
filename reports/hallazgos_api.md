# Hallazgos del EDA — fuente API (IPM)

- La API entregó 87,060 hogares y 33 columnas.
- Columnas con más nulos una vez convertido el texto NA: periodo (77.9%), p1075 (77.9%), p1077s22 (77.9%), p1077s21 (77.9%), p1077s23 (77.9%). Las casi vacías se descartan en la transformación.
- Duplicados exactos: 0; duplicados por llave directorio + secuencia_p: 0.
- Consistencia ipm_coincide: 100.00% de las filas.
- Consistencia pobre_coincide: 100.00% de las filas.
- Consistencia fexp_coincide: 100.00% de las filas.
- Atípicos por rango intercuartílico: personas (0.6%), fex_c (8.7%), fexp (9.9%), ipm (0.3%). Se conservan porque los factores de expansión varían por el diseño de la muestra.
- Hogares pobres: 8.3%; personas en pobreza multidimensional: 10.1%.
- La privación más frecuente es empleo_formal (73.4% de las personas) y la menos frecuente es trabajo_infantil (1.8%).
- Mayor pobreza multidimensional: Vichada (55.2% de las personas). Menor: Bogotá D.C. (2.2%).
- Departamentos con menos de 30 hogares en la muestra: 0.
