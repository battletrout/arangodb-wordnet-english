from pyArango.connection import *


def connect_to_arangodb(arangoURL='http://127.0.0.1:8529', username='wordnet_user', password=''):
    '''
    Creates a connection to the ArangoDB server with specified credentials. 
    If no credentials are specified, the defaults are used:
    arangoURL='http://127.0.0.1:8529', username='wordnet_user', password=''
    You will need to create a user named 'wordnet_user' with password '' in the ArangoDB server, 
    or input existing credentials.
    '''
    
    conn = Connection(arangoURL, username, password)

    return conn


# Testing out the connection and functionality for creating db, collections, and documents.
def conn_validation(conn, db_name):
    if not conn.hasDatabase(db_name):
        conn.createDatabase(name=db_name)
    db = conn[db_name] # all databases are loaded automatically into the connection and are accessible in this fashion

    if not conn.hasDatabase('test_db'):
        conn.createDatabase(name="test_db")
    db = conn["test_db"] # all databases are loaded automatically into the connection and are accessible in this fashion

    if not db.hasCollection('users'):
        db.createCollection(name="users") # all collections are also loaded automatically
    collection = db["users"]
    



    for i in range(100):
        doc = collection.createDocument({'_key': f"lessers{i}", 'cat' : 'meow'})
        #collection.createRel
        doc["name"] = f"Tesla-{i}"
        doc["number"] = i
        doc["species"] = "human"
        doc.save()

    doc = collection.createDocument()
    doc["name"] = "Tesla-101"
    doc["number"] = 101
    doc["species"] = "human"

if __name__ == '__main__':
    conn = connect_to_arangodb()
    conn_validation(conn, 'test_db')
    # conn = create_connection()
    # conn_validation(conn, 'wordnet_db')
