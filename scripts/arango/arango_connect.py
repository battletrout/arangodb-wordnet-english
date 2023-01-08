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

if __name__ == '__main__':
    conn = connect_to_arangodb()

    # conn = create_connection()
    # conn_validation(conn, 'wordnet_db')
