from pymongo import MongoClient

client= MongoClient('localhost', 27017)
db = client['db_practica']
collection = db['libros']

result_a = collection.find({
    "nombre_autor": "Gorka Leon",
    "anyo_edicion": {"$gt":2020}
})

for document in result_a:
    print(str("A:") + document)

result_b = collection.find({
    "nombre_autor": "Oihan Sancet",
    "titulo": {"$regex": "^The"}
})

for document in result_b:
    print(str("B:") + "document")