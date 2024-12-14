import streamlit as st
import pandas as pd
import altair as alt
import psycopg2
from sqlalchemy import create_engine
import os
# Configurar la página de Streamlit
st.set_page_config(page_title="Video Tracking Analysis", layout="wide")

# Deshabilitar el límite de filas en Altair
alt.data_transformers.disable_max_rows()

# Título de la aplicación
st.title("Análisis de Rastreo de Videos")


# Ruta del archivo SQLite
# db_path = "./tracking_results.db"  # Cambia esto al path donde está tu archivo .db

# Configuración de la base de datos PostgreSQL
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_port = os.getenv('DB_PORT')

database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(database_url)


try:
    st.success("Conexión exitosa a la base de datos PostgreSQL")
    # Conectarse a la base de datos SQLite
    # conn = sqlite3.connect(db_path)
    # st.write(f"Conectado a la base de datos: `{db_path}`")
    st.write(f"Conectado a la base de datos PostgreSQL: `{db_name}`")
    # Obtener las tablas disponibles
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    tables = pd.read_sql(query, engine)


    # Mostrar las tablas
    st.write("### Tablas en la base de datos:")
    st.write(tables)

    # Seleccionar una tabla para procesar
    selected_table = st.selectbox("Selecciona una tabla para analizar:", tables['table_name'])


    if selected_table:
        # Leer los datos de la tabla seleccionada
        df = pd.read_sql(f"SELECT * FROM {selected_table}", engine)

        st.write("### Datos Originales")
        st.dataframe(df)

        # Verificar si las columnas necesarias existen
        if not {'track_id', 'duration', 'direction'}.issubset(df.columns):
            st.error("La tabla seleccionada no contiene las columnas necesarias: 'track_id', 'duration', 'direction'.")
            st.stop()

        # Extraer las partes del 'track_id'
        df[['id_person', 'location', 'timestamp']] = df['track_id'].str.extract(r'(\d+)_(.+)-(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')

        # Convertir 'timestamp' a datetime
        df['datetime'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        if df['datetime'].isnull().any():
            st.error("Error al convertir 'timestamp' en datetime. Verifica el formato del track_id.")
            st.stop()

        # Convertir 'duration' a segundos
        df['duration'] = pd.to_numeric(df['duration'], errors='coerce') / 1000  # Convertir de ms a segundos

        # Mapear direcciones
        dir_map = {
            'forward': 'Hacia Garcia Roel',
            'backward': 'Hacia Luis Elizondo',
        }
        df['direction'] = df['direction'].map(dir_map)

        # Filtrar duraciones mayores a 30 segundos
        df = df[df['duration'] < 30]

        # Agregar columnas de hora y día de la semana
        df['hour'] = df['datetime'].dt.hour
        df['day_of_week'] = df['datetime'].dt.day_name()

        # Mapear días de la semana a español
        day_map = {
            'Monday': 'Lunes',
            'Tuesday': 'Martes',
            'Wednesday': 'Miércoles',
            'Thursday': 'Jueves',
            'Friday': 'Viernes',
            'Saturday': 'Sábado',
            'Sunday': 'Domingo'
        }
        df['day_of_week'] = df['day_of_week'].map(day_map)

        # Definir el orden correcto de los días
        day_order = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']

        # Mostrar datos procesados
        st.write("### Datos ")
        st.dataframe(df)


        # Filtro por día de la semana
        st.write("### Filtrar por día de la semana")
        selected_days = st.multiselect(
            "Selecciona los días que quieres incluir:",
            options=day_order,
            default=day_order
        )
        
        # TODO: Filtrar por direccion todos excpeto los que ya tienen la direccion integrada en el grafico ---------------------------------------->

        # Filtrar el DataFrame según los días seleccionados
        if selected_days:
            filtered_df = df[df['day_of_week'].isin(selected_days)]
        else:
            st.warning("No se seleccionó ningún día. Mostrando todos los datos.")
            filtered_df = df

        # Gráficos
        st.write("### Gráficos Generados")

        # Gráfico de área (duración media por hora y día)
        area_chart = alt.Chart(filtered_df).mark_area(opacity=0.5).encode(
            x=alt.X('hour:O', title="Hora del día (0-23)"),
            y=alt.Y('median(duration):Q', title='Duración media (s)'),
            color=alt.Color('day_of_week:N', title='Día de la semana', sort=day_order),
            tooltip=['hour', 'day_of_week', 'median(duration)']
        ).properties(title="Duración media por hora y por día de la semana")
        st.altair_chart(area_chart, use_container_width=True)

        # Gráfico hexagonal (Duración media por hora y día)
        size = 25  # Aumentamos el tamaño de los hexágonos
        xFeaturesCount = 24  # Número de horas en un día
        yFeaturesCount = 7   # Número de días en la semana
        hexagon = "M0,-2.3094010768L2,-1.1547005384 2,1.1547005384 0,2.3094010768 -2,1.1547005384 -2,-1.1547005384Z"

        # Crear gráfico hexagonal ajustado
        hex_duration_chart = alt.Chart(filtered_df, title="Duración media en cruzar por hora y por día de la semana").mark_point(
            size=size**2,
            shape=hexagon
        ).encode(
            alt.X('hour:O', title='Hora del día (0-23)',
                axis=alt.Axis(grid=False, tickOpacity=0, domainOpacity=0, labelFontSize=10, titleFontSize=12, labelColor='black', titleColor='black')),
            alt.Y('day_of_week:O', title='Día de la semana', sort=day_order,
                axis=alt.Axis(labelPadding=10, labelFontSize=10, titleFontSize=12, labelColor='black', titleColor='black')),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.5),
            fill=alt.Fill('median(duration):Q', title='Duración media (s)',
                        scale=alt.Scale(scheme='blues')),
            tooltip=[
                alt.Tooltip('hour:O', title='Hora'),
                alt.Tooltip('median(duration):Q', title='Duración media (s)')
            ]
        ).transform_calculate(
            # Asegurar un correcto posicionamiento del hexágono en X
            xFeaturePos='(1) / 2 + datum.hour'
        ).properties(
            width=size * xFeaturesCount * 3,
            height=size * yFeaturesCount * 2,
            background='white'  # Fondo blanco para contraste
        ).configure_view(
            strokeWidth=0
        ).configure_axis(
            domain=False
        ).configure_title(
            fontSize=14,
            font='Arial',
            color='black'
        ).configure_legend(
            titleColor='black',  # Color del título de la escala de colores
            labelColor='black',  # Color de las etiquetas de la escala de colores
            titleFontSize=12,
            labelFontSize=10
        )


        st.altair_chart(hex_duration_chart, use_container_width=True)
        
        
        # Gráfico de línea (duración media por día y hora)
        line_chart = alt.Chart(filtered_df).mark_line(
            point=alt.OverlayMarkDef(filled=False, fill="white")
        ).encode(
            x=alt.X('hour:O', title="Hora del día (0-23)", axis=alt.Axis(labelAngle=0)),
            y=alt.Y('median(duration):Q', title='Duración media (s)', scale=alt.Scale(zero=False)),
            color=alt.Color('day_of_week:N', title='Día de la semana', legend=alt.Legend(title="Día de la semana"), sort=day_order),
            tooltip=['hour', 'day_of_week', 'median(duration)']
        ).properties(title="Duración media por día y hora", width=600, height=400)
        st.altair_chart(line_chart, use_container_width=True)

        # Gráfico de barras (dirección por duración media)
        bar_chart = alt.Chart(filtered_df).mark_bar(opacity=0.3, binSpacing=0).encode(
            x=alt.X('hour:O', title="Hora del día (0-23)"),
            y=alt.Y('median(duration)', title='Duración media'),
            color=alt.Color('direction:N', title='Dirección'),
            tooltip=['median(duration)']
        ).properties(title="Duración media por dirección")
        st.altair_chart(bar_chart, use_container_width=True)

        # Gráfico de área (conteo de personas por hora y día)
        count_area_chart = alt.Chart(filtered_df).mark_area(opacity=0.5).encode(
            x=alt.X('hour:O', title="Hora del día (0-23)"),
            y=alt.Y('count():Q', title='Número de personas'),
            color=alt.Color('day_of_week:N', title='Día de la semana', sort=day_order),
            tooltip=['hour', 'day_of_week', 'count()']
        ).properties(title="Número de personas por hora y día de la semana")
        st.altair_chart(count_area_chart, use_container_width=True)

        # Gráfico de línea (Conteo por día y hora)
        count_line_chart = alt.Chart(filtered_df).mark_line(
            point=alt.OverlayMarkDef(filled=False, fill="white")
        ).encode(
            x=alt.X('hour:O', title="Hora del día (0-23)", axis=alt.Axis(labelAngle=0)),
            y=alt.Y('count():Q', title='Conteo de personas', scale=alt.Scale(zero=False)),
            color=alt.Color('day_of_week:N', title='Día de la semana', legend=alt.Legend(title="Día de la semana"), sort=day_order),
            tooltip=['hour', 'day_of_week', 'count()']
        ).properties(
            title="Conteo por día y hora",
            width=600,
            height=400
        )
        st.altair_chart(count_line_chart, use_container_width=True)
        
        
        # Gráfico de barras por dirección con estilo consistente
        direction_bar_chart = alt.Chart(filtered_df).mark_bar(
            opacity=0.3,  # Opacidad ajustada para mantener el estilo consistente
            binSpacing=0  # Sin espacio entre las barras
        ).encode(
            x=alt.X('hour:O', title="Hora del día (0-23)", axis=alt.Axis(labelFontSize=12, titleFontSize=14)),
            y=alt.Y('count():Q', title='Número de personas'),
            color=alt.Color('direction:N', title='Dirección'),
            tooltip=[
                alt.Tooltip('hour:O', title='Hora del día'),
                alt.Tooltip('direction:N', title='Dirección'),
                alt.Tooltip('count():Q', title='Número de personas')
            ]
        ).properties(
            title="Distribución del Número de Personas por Dirección y Hora",  # Título del gráfico
            width=800,  # Ancho del gráfico
            height=400,  # Altura del gráfico
            
        ).configure_view(
            strokeWidth=0  # Sin bordes para el área del gráfico
        ).configure_axis(
            domain=False  # Sin líneas del dominio
        ).configure_title(
            fontSize=14,
            font='Arial',
            color='white'
        ).configure_legend(
            titleColor='white',  # Color del título de la leyenda
            labelColor='white',  # Color de las etiquetas de la leyenda
            titleFontSize=12,
            labelFontSize=10
        )

        # Renderizar en Streamlit
        st.altair_chart(direction_bar_chart, use_container_width=True)



        
        # Gráfico hexagonal (Conteo de personas por hora y día)
        size = 25  # Tamaño del hexágono consistente con el gráfico de duración media
        xFeaturesCount = 24  # Número de horas en un día
        yFeaturesCount = 7   # Número de días en la semana
        hexagon = "M0,-2.3094010768L2,-1.1547005384 2,1.1547005384 0,2.3094010768 -2,1.1547005384 -2,-1.1547005384Z"

        # Crear gráfico
        hex_chart = alt.Chart(filtered_df, title="Conteo de personas por hora y día de la semana").mark_point(
            size=size**2,
            shape=hexagon
        ).encode(
            alt.X('hour:O', title="Hora del día (0-23)",
                axis=alt.Axis(grid=False, tickOpacity=0, domainOpacity=0, labelFontSize=10, titleFontSize=12, labelColor='black', titleColor='black')),
            alt.Y('day_of_week:O', title='Día de la semana', sort=day_order,
                axis=alt.Axis(labelPadding=10, labelFontSize=10, titleFontSize=12, labelColor='black', titleColor='black')),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.5),
            fill=alt.Fill('count():Q', title='Conteo',
                        scale=alt.Scale(scheme='blues')),
            tooltip=[
                alt.Tooltip('hour:O', title='Hora'),
                alt.Tooltip('count():Q', title='Conteo de personas')
            ]
        ).properties(
            width=size * xFeaturesCount * 3,  # Ancho consistente con el gráfico de duración media
            height=size * yFeaturesCount * 2,  # Altura consistente con el gráfico de duración media
            background='white'  # Fondo blanco
        ).configure_view(
            strokeWidth=0
        ).configure_axis(
            domain=False
        ).configure_title(
            fontSize=14,
            font='Arial',
            color='black'
        ).configure_legend(
            titleColor='black',  # Título de escala de colores en negro
            labelColor='black',  # Etiquetas de escala en negro
            titleFontSize=12,
            labelFontSize=10
        )

        # Renderizar en Streamlit
        st.altair_chart(hex_chart, use_container_width=True)

except Exception as e:
    st.error(f"Error al procesar los datos: {e}")

