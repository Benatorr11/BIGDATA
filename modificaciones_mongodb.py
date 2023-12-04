import pandas as pd
from pymongo import MongoClient

# Conexión a MongoDB (asegúrate de tener el servidor MongoDB en ejecución)
client = MongoClient('localhost', 27017)
db = client['db_practica']  # Reemplaza con el nombre de tu base de datos
collection = db['libros']

# Cargar el conjunto de datos desde el archivo CSV
df = pd.read_csv('books.csv', delimiter=';', nrows = 100, encoding='latin-1')

# Modificar el nombre del autor de "Mark P. O. Morford" a "Gorka Leon"
df.loc[df['Book-Author'] == 'Mark P. O. Morford', 'Book-Author'] = 'MARK P. O. MORFORD'

# Modificar el nombre del autor de "Richard Bruce Wright" a "Oihan Sancet"
df.loc[df['Book-Author'] == 'Richard Bruce Wright', 'Book-Author'] = 'RICHARD BRUCE WRIGHT'

# Convertir el DataFrame modificado a registros JSON
records = df.to_dict(orient='records')

# Borrar la colección existente antes de insertar los registros actualizados
collection.drop()

# Insertar los registros en la colección 'libros'
collection.insert_many(records)

print("Inserción completada para los registros modificados.")

# visualizacion de las 5 primeras filas
cursor = collection.find().limit(5)
for document in cursor:
    print(document)