import pandas as pd
import sqlite3

# Extract
df = pd.read_csv(r'C:\Users\usuario\OneDrive\Desktop\Migracion y Testing\icfes (1).txt', sep='|', low_memory=False)  # Usa tu path

# Transform: Limpia (convierte numéricos, maneja nulos)
numeric_cols = ['ESTU_NACIMIENTO_DIA', 'ESTU_NACIMIENTO_MES', 'ESTU_NACIMIENTO_ANNO', 'ESTU_EDAD', 'ESTU_VECES_ESTADO',
                'ESTU_TOTAL_ALUMNOS_CURSO', 'ESTU_ANO_MATRICULA_PRIMERO', 'ESTU_ANO_TERMINO_QUINTO', 'ESTU_ANOS_COLEGIO_ACTUAL',
                'ESTU_ANO_MATRICULA_SEXTO', 'ESTU_ANOS_PREESCOLAR', 'ESTU_CUANTOS_COLE_ESTUDIO', 'FAMI_PERSONAS_HOGAR',
                'FAMI_CUARTOS_HOGAR', 'ESTU_HORAS_TRABAJA', 'PUNT_LENGUAJE', 'PUNT_MATEMATICAS', 'PUNT_C_SOCIALES',
                'PUNT_FILOSOFIA', 'PUNT_BIOLOGIA', 'PUNT_QUIMICA', 'PUNT_FISICA', 'PUNT_INGLES', 'PUNT_COMP_FLEXIBLE',
                'ESTU_PUESTO']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

df = df.fillna('')  # Maneja nulos como strings vacíos para evitar errores en SQL

# Selecciona todas las columnas (ya que son relevantes para un ETL completo)
# Pero si quieres filtrar, quita algunos no usados en requisitos

# Agrupa tablas
df_colegios = df[['COLE_COD_ICFES', 'COLE_COD_DANE_INSTITUCION', 'COLE_NOMBRE_SEDE', 'COLE_CALENDARIO',
                  'COLE_GENERO', 'COLE_NATURALEZA', 'COLE_BILINGUE', 'COLE_JORNADA', 'COLE_CARACTER',
                  'COLE_VALOR_PENSION']].drop_duplicates(subset=['COLE_COD_ICFES'])

df_estudiantes = df[['ESTU_CONSECUTIVO', 'PERIODO', 'ESTU_TIPODOCUMENTO', 'ESTU_PAIS_RESIDE', 'ESTU_GENERO',
                     'ESTU_NACIMIENTO_DIA', 'ESTU_NACIMIENTO_MES', 'ESTU_NACIMIENTO_ANNO', 'ESTU_EDAD',
                     'ESTU_LIMITA_BAJAVISION', 'ESTU_LIMITA_SORDOCEGUERA', 'ESTU_LIMITA_COGNITIVA',
                     'ESTU_LIMITA_INVIDENTE', 'ESTU_LIMITA_MOTRIZ', 'ESTU_LIMITA_SORDOINTERPRETE',
                     'ESTU_LIMITA_SORDONOINTERPRETE', 'ESTU_ETNIA', 'ESTU_COD_RESIDE_MCPIO', 'ESTU_RESIDE_MCPIO',
                     'ESTU_RESIDE_DEPTO', 'ESTU_ZONA_RESIDE', 'ESTU_AREA_RESIDE', 'IND_COD_ICFES_TERMINO',
                     'COLE_COD_ICFES', 'ESTU_VECES_ESTADO', 'ESTU_CARRDESEADA_TIPO', 'ESTU_IES_COD_DESEADA',
                     'ESTU_IES_COD_MPIO_DESEADA', 'ESTU_IES_DEPT_DESEADA', 'ESTU_IES_DESEADA_NOMBRE',
                     'ESTU_IES_MPIO_DESEADA', 'ESTU_TOTAL_ALUMNOS_CURSO', 'ESTU_ANO_MATRICULA_PRIMERO',
                     'ESTU_ANO_TERMINO_QUINTO', 'ESTU_ANOS_COLEGIO_ACTUAL', 'ESTU_ANO_MATRICULA_SEXTO',
                     'ESTU_ANOS_PREESCOLAR', 'ESTU_CUANTOS_COLE_ESTUDIO', 'ESTU_REPROBO_CUARTO',
                     'ESTU_REPROBO_DECIMO', 'ESTU_REPROBO_NOVENO', 'ESTU_REPROBO_OCTAVO',
                     'ESTU_REPROBO_PRIMERO', 'ESTU_REPROBO_QUINTO', 'ESTU_REPROBO_SEGUNDO',
                     'ESTU_REPROBO_SEPTIMO', 'ESTU_REPROBO_SEXTO', 'ESTU_REPROBO_TERCERO',
                     'ESTU_REPROBO_ONCE_MAS', 'ESTU_POR_MEJORARPOSICIONSOCIAL', 'ESTU_POR_COLOMBIAAPRENDE',
                     'ESTU_POR_INFLUENCIAALGUIEN', 'ESTU_POR_INTERESPERSONAL', 'ESTU_POR_BUSCANDOCARRERA',
                     'ESTU_POR_TRADICIONFAMILIAR', 'ESTU_POR_ORIENTACIONVOCACIONAL', 'ESTU_RAZON_RETIRO',
                     'ESTU_POR_AMIGOSESTUDIANDO', 'ESTU_POR_COSTOMATRICULA', 'ESTU_POR_OPORTUNIDADES',
                     'ESTU_POR_OTRARAZON', 'ESTU_PRESTIGIOINSTITUCION', 'ESTU_POR_UBICACION',
                     'ESTU_POR_UNICAQUEOFRECE', 'ESTU_RETIRARSE_COLEGIO', 'ESTU_COD_MCPIO_PRESENTACION',
                     'ESTU_MCPIO_PRESENTACION', 'ESTU_DEPTO_PRESENTACION', 'ESTU_EXAM_NOMBREEXAMEN',
                     'FAMI_EDUCA_PADRE', 'FAMI_EDUCA_MADRE', 'FAMI_OCUPA_PADRE', 'FAMI_OCUPA_MADRE',
                     'FAMI_ESTRATO_VIVIENDA', 'FAMI_NIVEL_SISBEN', 'FAMI_PERSONAS_HOGAR', 'FAMI_CUARTOS_HOGAR',
                     'FAMI_PISOSHOGAR', 'FAMI_TELEFONO_FIJO', 'FAMI_CELULAR', 'FAMI_INTERNET',
                     'FAMI_SERVICIO_TELEVISION', 'FAMI_COMPUTADOR', 'FAMI_LAVADORA', 'FAMI_NEVERA',
                     'FAMI_HORNO', 'FAMI_DVD', 'FAMI_MICROONDAS', 'FAMI_AUTOMOVIL',
                     'FAMI_INGRESO_FMILIAR_MENSUAL', 'ESTU_TRABAJA', 'ESTU_HORAS_TRABAJA']]

df_puntajes = df[['ESTU_CONSECUTIVO', 'PUNT_LENGUAJE', 'PUNT_MATEMATICAS', 'PUNT_C_SOCIALES',
                  'PUNT_FILOSOFIA', 'PUNT_BIOLOGIA', 'PUNT_QUIMICA', 'PUNT_FISICA',
                  'PUNT_INGLES', 'DESEMP_INGLES', 'NOMBRE_COMP_FLEXIBLE', 'PUNT_COMP_FLEXIBLE',
                  'DESEMP_COMP_FLEXIBLE', 'ESTU_PUESTO']]

# Load
conn = sqlite3.connect('icfes.db')

df_colegios.to_sql('Colegios', conn, if_exists='append', index=False)
df_estudiantes.to_sql('Estudiantes', conn, if_exists='append', index=False)
df_puntajes.to_sql('Puntajes', conn, if_exists='append', index=False)

conn.close()
print("ETL completado con todos los campos.")

