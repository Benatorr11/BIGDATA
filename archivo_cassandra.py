import pandas as pd
from cassandra.cluster import Cluster

# Conexión a Cassandra (asegúrate de tener el servidor Cassandra en ejecución)
cluster = Cluster(['localhost'])
session = cluster.connect('keyCASS')  # Reemplaza con el nombre de tu keyspace

# Cargar el dataset desde el archivo CSV
df = pd.read_csv('books.csv', encoding='latin-1', nrows=100, delimiter=';')  # Reemplaza con la ruta correcta a tu archivo CSV

# Iterar sobre el DataFrame y realizar las inserciones en Cassandra
for index, row in df.iterrows():
    session.execute(
        """
        INSERT INTO libros (isbn, titulo, anyo_edicion, autores)
        VALUES (%s, %s, %s, %s)
        """,
        (row['isbn'], row['titulo'], row['anyo_edicion'], [row['id_autor'], row['nombre_autor']])
    )

print("Inserción completada para los primeros 100 registros.")
