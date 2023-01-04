from pyArango.connection import *


conn = Connection(arangoURL='http://127.0.0.1:8529', username='wordnet_user', password='password')

if not conn.hasDatabase('test_db'):
    conn.createDatabase(name="test_db")
db = conn["test_db"] # all databases are loaded automatically into the connection and are accessible in this fashion

if not db.hasCollection('users'):
    db.createCollection(name="users") # all collections are also loaded automatically
collection = db["users"]

for i in range(100):
    doc = collection.createDocument()
    doc["name"] = f"Tesla-{i}"
    doc["number"] = i
    doc["species"] = "human"
    doc.save()

doc = collection.createDocument()
doc["name"] = "Tesla-101"
doc["number"] = 101
doc["species"] = "human"
