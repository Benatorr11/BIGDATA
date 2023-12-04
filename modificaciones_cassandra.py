import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Configuración de la conexión a Cassandra
cluster = Cluster(['localhost'])  # Reemplaza 'localhost' con la dirección de tu cluster de Cassandra
session = cluster.connect()
session.set_keyspace('keyspace_practica')  # Reemplaza 'keyspace_practica' con el nombre de tu keyspace

# Cargar el conjunto de datos desde el archivo CSV
df = pd.read_csv('books.csv', delimiter=';', nrows=100, encoding='latin-1')

# Modificar el nombre del autor de "Mark P. O. Morford" a "Gorka Leon"
df.loc[df['Book-Author'] == 'Mark P. O. Morford', 'Book-Author'] = 'MARK P. O. MORFORD'

# Modificar el nombre del autor de "Richard Bruce Wright" a "Oihan Sancet"
df.loc[df['Book-Author'] == 'Richard Bruce Wright', 'Book-Author'] = 'RICHARD BRUCE WRIGHT'

# Crear el esquema de la tabla en Cassandra (asumiendo que aún no existe)
create_table_query = """
    CREATE TABLE IF NOT EXISTS libros (
        book_id UUID PRIMARY KEY,
        book_title TEXT,
        book_author TEXT,
        book_format TEXT,
        publication_year INT
    )
"""
session.execute(create_table_query)

# Insertar los registros en la tabla 'libros'
insert_query = """
    INSERT INTO libros (book_id, book_title, book_author, book_format, publication_year)
    VALUES (?, ?, ?, ?, ?)
"""

for index, row in df.iterrows():
    session.execute(insert_query, (uuid.uuid1(), row['Book-Title'], row['Book-Author'], row['Book-Format'], row['Publication-Year']))

print("Inserción completada para los registros modificados.")

# Visualización de las 5 primeras filas
select_query = "SELECT * FROM libros LIMIT 5"
rows = session.execute(select_query)
for row in rows:
    print(row)
