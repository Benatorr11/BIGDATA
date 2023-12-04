import pandas as pd
from pymongo import MongoClient

# Conexión a MongoDB (asegúrate de tener el servidor MongoDB en ejecución)
client = MongoClient('localhost', 27017)
db = client['db_practica']  # Reemplaza con el nombre de tu base de datos
collection = db['libros']

# Cargar el dataset desde el archivo CSV
df = pd.read_csv('books.csv', encoding='latin-1', nrows = 100, delimiter = ';')  # Reemplaza con la ruta correcta a tu archivo CSV

# Seleccionar los primeros 100 registros del dataset
subset_df = df.head(100)

# Convertir los registros a formato JSON
records = subset_df.to_dict(orient='records')

# Insertar los registros en la colección 'libros'
collection.insert_many(records)

print("Inserción completada para los primeros 100 registros.")
