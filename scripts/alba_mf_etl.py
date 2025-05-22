import pandas as pd
import sqlite3
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'alba_mf_etl.log')),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("alba_mf_etl")

# Configuración de rutas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW = os.path.join(BASE_DIR, 'data', 'raw')
DATA_PROCESSED = os.path.join(BASE_DIR, 'data', 'processed')
DATA_FINAL = os.path.join(BASE_DIR, 'data', 'final')

# Asegurar que los directorios existen
for dir_path in [DATA_RAW, DATA_PROCESSED, DATA_FINAL, os.path.join(BASE_DIR, 'logs')]:
    os.makedirs(dir_path, exist_ok=True)

class ETLPipeline:
    def __init__(self):
        self.db_path = os.path.join(DATA_RAW, 'mf_data.db')
        
    def extract_from_excel_ec_data(self):
        # Extrae datos un Excel
        try:
            # Extracción
            excel_path = os.path.join(DATA_RAW, 'alba_mf.xlsm')
            df = pd.read_excel(excel_path, sheet_name='EC DATA')

            # Guardar en un CSV
            alba_mf_path = os.path.join(DATA_RAW, 'ec_data_alba.csv')
            df.to_csv(alba_mf_path, index=False)
            logger.info(f"Datos extraidos de Excel: {df.shape}")

            return df
        except Exception as e:
            logger.error(f"Error al extraer datos de Excel: {str(e)}")

    def extract(self):
        # Coordina la extracción de datos de todas las fuentes
        try:
            logger.info("Iniciando la extración de datos")

            # Extraer datos de diferentes fuentes
            df_ec_data_alba = self.extract_from_excel_ec_data()

            # Guardar los datos extraidos
            return {
                'ec_data_alba': df_ec_data_alba
            }
        
        except Exception as e:
            logger.error(f"Error en la extracción de datos: {str(e)}")

    def transform(self, data_dict):
        # Transforma y combina los datos extraidos
        try:
            logger.info("Iniciando la transformación de datos")

            # Extraer los DataFrame
            df_ec_data_alba = data_dict['ec_data_alba']

            # 1. Prepara datos de energy components
            # Obtener los nombres de columna de la segunda fila y las colunmas de fechas
            column_names = df_ec_data_alba.iloc[1].values.tolist()
            columns_to_rename = [1, 18, 35, 47, 55]

            # Convertir todos los nombres de columnas a strings primero
            column_names = [str(col) for col in column_names]

            # Ahora aplicar la transformación a minúsculas y guiones bajos
            column_names = [col.lower().replace(' ', '_').replace('-', '_') for col in column_names]

            # Renombrar las columnas con fechas
            for i, pos in enumerate(columns_to_rename):
                column_names[pos] = f'date_{i + 1}'

            # Asignar los nuevos nombres de columna
            df_ec_data_alba.columns = column_names

            # Eliminar las dos primeras filas (índices 0 y 1) y restablecer índices
            df_ec_data_alba = df_ec_data_alba.drop(df_ec_data_alba.index[:2]).reset_index(drop=True)

            # Eliminar columnas innecesarias
            df_ec_data_alba = df_ec_data_alba.iloc[:, 1:50]

            # Convertir las fechas a datetime pero sin el time
            date_columns = [col for col in df_ec_data_alba.columns if 'date' in col]
            for col in date_columns:
                df_ec_data_alba[col] = pd.to_datetime(df_ec_data_alba[col], errors='coerce').dt.floor('D')

            # Eliminar columnas NaN
            columnas_nan = [col for col in df_ec_data_alba.columns if col == "nan"]
            if columnas_nan:
                df_ec_data_alba = df_ec_data_alba.drop(columns=columnas_nan)

            # Convertir las columnas con valores numericos a float   
            columns_to_exclude = date_columns + ['tank_name', 'product']
            for col in df_ec_data_alba.columns:
                if col not in columns_to_exclude:
                    df_ec_data_alba[col] = pd.to_numeric(df_ec_data_alba[col], errors='coerce')

            # 2. Separar datos en diferentes df
            # LIQUID HYDROCARBONS CACHED
            df_liquid_hydrocarbons_cached = df_ec_data_alba.iloc[:, 0:11]
            df_gas_production = df_ec_data_alba.iloc[:, 12:24]
            df_tank_data = df_ec_data_alba.iloc[:, 27:35]
            df_daily_lifting_data = df_ec_data_alba.iloc[:, 35:38]

            # 3. Limpiar cada df de forma independiente
            # 3.1 Limpieza de df_liquid_hydrocarbons_cached
            # Eliminar las filas nulas
            df_liquid_hydrocarbons_cached = df_liquid_hydrocarbons_cached.dropna(how='all')

            # Eliminar columnas en desuso y cambiar el nombre de la columna date
            lhc_columns_to_delete = ['eglng_propane_sales', 'llc_share_of_secondary_condensate', 'psc_share_of_secondary_condensate']
            df_liquid_hydrocarbons_cached = df_liquid_hydrocarbons_cached.drop(columns=lhc_columns_to_delete)
            df_liquid_hydrocarbons_cached = df_liquid_hydrocarbons_cached.rename(columns={'date_1': 'date'})

            # 3.2 Limpieza de df_gas_production
            # Eliminar las filas nulas
            df_gas_production = df_gas_production.dropna(how='all')

            # Eliminar columnas en desuso y cambiar el nombre de la columna date
            gp_columns = ['date_2', 'ampco_gas_sales', 'eglng_gas_sales', 'gas_sales', 'offshore_gas']
            df_gas_production = df_gas_production[gp_columns]
            df_gas_production = df_gas_production.rename(columns={'date_2': 'date'})

            # 3.3 Limpieza de df_tank_data
            # Eliminar las filas nulas
            df_tank_data = df_tank_data.dropna(how='all')

            # Eliminar columnas en desuso y cambiar el nombre de la columna date
            td_columns = ['date_3', 'tank_name', 'standard_net_oil_volume_(bbls)']
            df_tank_data = df_tank_data[td_columns]
            df_tank_data = df_tank_data.rename(columns={'date_3': 'date'})

            # 3.4 Limpieza de df_daily_lifting_data
            # Eliminar las filas nulas
            df_daily_lifting_data = df_daily_lifting_data.dropna(how='all')

            # Cambiar el nombre de la columna date
            df_daily_lifting_data = df_daily_lifting_data.rename(columns={'date_4': 'date'})

            # Combinar y guardar en un CSV
            # Con reinicio de índices
            # df_combinado = pd.concat([df_liquid_hydrocarbons_cached, df_gas_production, df_tank_data, df_daily_lifting_data], ignore_index=True)

            # # Mantener información sobre la fuente
            # df_liquid_hydrocarbons_cached['fuente'] = 'df_liquid_hydrocarbons_cached'
            # df_gas_production['fuente'] = 'df_gas_production'
            # df_tank_data['fuente'] = 'df_tank_data'
            # df_daily_lifting_data['fuente'] = 'df_daily_lifting_data'

            # df_combinado = pd.concat([df_liquid_hydrocarbons_cached, df_gas_production, df_tank_data, df_daily_lifting_data], ignore_index=True)
            # processed_path = os.path.join(DATA_PROCESSED, 'ec_data_alba.csv')
            # df_combinado.to_csv(processed_path, index=False)

            # Guardar
            df_liquid_hydrocarbons_cached.to_csv(os.path.join(DATA_PROCESSED, 'liquid_hydrocarbons_cached.csv'), index=False)
            df_gas_production.to_csv(os.path.join(DATA_PROCESSED, 'gas_production.csv'), index=False)
            df_tank_data.to_csv(os.path.join(DATA_PROCESSED, 'tank_data.csv'), index=False)
            df_daily_lifting_data.to_csv(os.path.join(DATA_PROCESSED, 'daily_lifting_data.csv'), index=False)

            logger.info(f"Datos extraidos de Excel: {df_ec_data_alba.shape}")

            return df_liquid_hydrocarbons_cached, df_gas_production, df_tank_data, df_daily_lifting_data
        except Exception as e:
            logger.error(f"Error en la transformación de datos: {str(e)}")

    def load(self, df_liquid_hydrocarbons_cached, df_gas_production, df_tank_data, df_daily_lifting_data):
        # Cargar los datos
        try:
            logger.info("Iniciando la carga de datos")

            # 1. Guardar los datos en CSV para fácil acceso
            df_liquid_hydrocarbons_cached.to_csv(os.path.join(DATA_FINAL, 'liquid_hydrocarbons_cached.csv'), index=False)
            df_gas_production.to_csv(os.path.join(DATA_FINAL, 'gas_production.csv'), index=False)
            df_tank_data.to_csv(os.path.join(DATA_FINAL, 'tank_data.csv'), index=False)
            df_daily_lifting_data.to_csv(os.path.join(DATA_FINAL, 'daily_lifting_data.csv'), index=False)

            # 2 Guardar en sqlite para consultas
            conn = sqlite3.connect(self.db_path)
            df_liquid_hydrocarbons_cached.to_sql('liquid_hydrocarbons_cached', conn, if_exists='replace', index=True, index_label="id")
            df_gas_production.to_sql('gas_production', conn, if_exists='replace', index=True, index_label="id")
            df_tank_data.to_sql('tank_data', conn, if_exists='replace', index=True, index_label="id")
            df_daily_lifting_data.to_sql('daily_lifting_data', conn, if_exists='replace', index=True, index_label="id")

            logger.info(f"Datos cargados y guardados en {self.db_path}.")
            
            return True
        except Exception as e:
            logger.error(f"Error en la carga de datos: {str(e)}")

    def run(self):
        # Ejecuta el pipeline
        try:
            logger.info("Iniciando pipeline")
            data_dict = self.extract()
            lhc, gp, td, dld = self.transform(data_dict)
            self.load(lhc, gp, td, dld)
            logger.info("Pipeline ETL completado con éxito.")
            return True
        except Exception as e:
            logger.error(f"Error en el proceso ETL: {str(e)}")
            return False
        
if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()