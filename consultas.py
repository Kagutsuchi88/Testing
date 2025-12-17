import sqlite3
import pandas as pd

# Conectar a la base de datos
conn = sqlite3.connect('icfes.db')

# Función para ejecutar y mostrar una consulta
def ejecutar_consulta(sql):
    print("\n" + "="*80)
    print("CONSULTA:")
    print(sql)
    print("="*80)
    df = pd.read_sql_query(sql, conn)
    print(df)
    print("="*80)
    return df

# Aquí pones todas las consultas de los requisitos

# 1. Persona con el mejor puntaje en cada materia (ejemplos para algunas)
print("1. Mejor puntaje por materia (ejemplos)")
ejecutar_consulta("""
SELECT ESTU_CONSECUTIVO, 'LENGUAJE' AS Materia, PUNT_LENGUAJE AS Puntaje
FROM Puntajes 
WHERE PUNT_LENGUAJE = (SELECT MAX(PUNT_LENGUAJE) FROM Puntajes)
UNION ALL
SELECT ESTU_CONSECUTIVO, 'MATEMATICAS', PUNT_MATEMATICAS
FROM Puntajes 
WHERE PUNT_MATEMATICAS = (SELECT MAX(PUNT_MATEMATICAS) FROM Puntajes)
UNION ALL
SELECT ESTU_CONSECUTIVO, 'INGLES', PUNT_INGLES
FROM Puntajes 
WHERE PUNT_INGLES = (SELECT MAX(PUNT_INGLES) FROM Puntajes)
-- Repite para las demás materias si quieres
""")

# 2. Colegio con el mejor puntaje promedio (promedio de las 8 áreas principales)
ejecutar_consulta("""
SELECT 
    c.COLE_NOMBRE_SEDE AS Colegio,
    ROUND(AVG(p.PUNT_LENGUAJE + p.PUNT_MATEMATICAS + p.PUNT_C_SOCIALES + 
              p.PUNT_FILOSOFIA + p.PUNT_BIOLOGIA + p.PUNT_QUIMICA + 
              p.PUNT_FISICA + p.PUNT_INGLES) / 8.0, 2) AS Promedio_General
FROM Puntajes p
JOIN Estudiantes e ON p.ESTU_CONSECUTIVO = e.ESTU_CONSECUTIVO
JOIN Colegios c ON e.COLE_COD_ICFES = c.COLE_COD_ICFES
GROUP BY c.COLE_NOMBRE_SEDE
ORDER BY Promedio_General DESC
LIMIT 5
""")

# 3. Cantidad de personas con nivel de inglés superior a B1
# Según el diccionario: B+ es superior a B1, B1 es B1. Superiores: solo B+
ejecutar_consulta("""
SELECT COUNT(*) AS Cantidad_Superior_B1
FROM Puntajes
WHERE DESEMP_INGLES = 'B+'
""")

# 4. Municipio con mayor cantidad de personas con nivel inglés superior a B1
ejecutar_consulta("""
SELECT 
    e.ESTU_MCPIO_PRESENTACION AS Municipio,
    COUNT(*) AS Cantidad
FROM Puntajes p
JOIN Estudiantes e ON p.ESTU_CONSECUTIVO = e.ESTU_CONSECUTIVO
WHERE p.DESEMP_INGLES = 'B+'
GROUP BY e.ESTU_MCPIO_PRESENTACION
ORDER BY Cantidad DESC
LIMIT 1
""")

# 5. Los cinco colegios NO bilingües con mejor puntaje promedio en inglés
ejecutar_consulta("""
SELECT 
    c.COLE_NOMBRE_SEDE AS Colegio,
    ROUND(AVG(p.PUNT_INGLES), 2) AS Promedio_Ingles
FROM Puntajes p
JOIN Estudiantes e ON p.ESTU_CONSECUTIVO = e.ESTU_CONSECUTIVO
JOIN Colegios c ON e.COLE_COD_ICFES = c.COLE_COD_ICFES
WHERE c.COLE_BILINGUE = 'N' OR c.COLE_BILINGUE = '0' OR c.COLE_BILINGUE IS NULL
GROUP BY c.COLE_NOMBRE_SEDE
ORDER BY Promedio_Ingles DESC
LIMIT 5
""")

# 6. Persona con mejor puntaje en matemáticas por género
ejecutar_consulta("""
SELECT 
    e.ESTU_GENERO AS Genero,
    e.ESTU_CONSECUTIVO,
    MAX(p.PUNT_MATEMATICAS) AS Mejor_Puntaje_Matematicas
FROM Puntajes p
JOIN Estudiantes e ON p.ESTU_CONSECUTIVO = e.ESTU_CONSECUTIVO
GROUP BY e.ESTU_GENERO
""")

conn.close()