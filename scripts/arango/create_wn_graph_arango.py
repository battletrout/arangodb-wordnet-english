from pyArango.connection import *
from arango_connect import connect_to_arangodb
from parse_xml import WordNetXMLParser


def get_db(conn, db_name):
    '''
    Input connection and db_name. Creates a new DB if doesn't exist, returns the db object.
    '''
    if not conn.hasDatabase(db_name):
        conn.createDatabase(name=db_name)
    db = conn[db_name] # all databases are loaded automatically into the connection and are accessible in this fashion
    return db

def get_collection(db, collection_name):
    '''
    Input db and collection_name. Creates a new collection if doesn't exist, returns the collection object.
    '''
    if not db.hasCollection(collection_name):
        db.createCollection(name=collection_name) # all collections are also loaded automatically
    collection = db[collection_name]
    return collection

def get_edge_collection(db, collection_name):
    '''
    Input db and collection_name. Creates a new collection if doesn't exist, returns the collection object.
    '''
    if not db.hasCollection(collection_name):
        db.createCollection(name=collection_name, className='Edges') # all collections are also loaded automatically
    collection = db[collection_name]
    return collection

def add_all_sense_ids(collection, sense_id_dict):
    for sense_id, attributes in sense_id_dict.items():
        sense_doc = collection.createDocument({'_key': sense_id})
        for attribute, value in attributes.items():
            sense_doc[attribute] = value
        sense_doc.save()

def add_all_lex_entries(collection, lex_entry_dict):
    for lex_entry, attributes in lex_entry_dict.items():
        lex_doc = collection.createDocument({'_key': lex_entry})
        for attribute, value in attributes.items():
            lex_doc[attribute] = value
        lex_doc.save()

def add_all_synsets(collection, synset_dict):
    for synset, attributes in synset_dict.items():
        synset_doc = collection.createDocument({'_key': synset})
        for attribute, value in attributes.items():
            synset_doc[attribute] = value
        synset_doc.save()

def add_all_edges(collection, edge_list):
    for edge in edge_list:
        edge_doc = collection.createDocument()
        for attribute, value in edge.items():
            edge_doc[attribute] = value
        edge_doc.save()

#         self.sense_id_dict = {}
#         self.lex_entry_dict = {}
#         self.synset_dict = {}

#         # edge_list is a list of dicts where values are source, type (of relationship), and target.
#         self.edge_list = []


def main():
    conn = connect_to_arangodb()
    db = get_db(conn, 'wordnet_db')
    sense_id_collection = get_collection(db, 'sense_ids')
    lex_entry_collection = get_collection(db, 'lex_entries')
    synset_collection = get_collection(db, 'synsets')
    edge_collection = get_edge_collection(db, 'edges')

    parser = WordNetXMLParser('wn.xml')
    parser.parse()
    add_all_sense_ids(sense_id_collection, parser.sense_id_dict)
    add_all_lex_entries(lex_entry_collection, parser.lex_entry_dict)
    add_all_synsets(synset_collection, parser.synset_dict)
    add_all_edges(edge_collection, parser.edge_list)


if __name__ == '__main__':
    main()