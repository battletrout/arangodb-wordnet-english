from pyArango.connection import *
from pyArango.collection import Collection, Edges
from arango_connect import connect_to_arangodb
from parse_xml import WordNetXMLParser
from datetime import datetime

class ArangoDBGraphCreator:
    class UnexpectedRelationType(Exception):
        pass

    def __init__(self, db_name='wordnet_db', connection=connect_to_arangodb()):
        self.connection = connection
        self.db_name = db_name

    def create_ArangoDB_WordNet_from_XML(self, xml_filepath='wn.xml',written_form_in_sense_id=True, pos_in_sense_id=True):
        '''
        Main function that creates the WordNet graph's collections in ArangoDB from XML filepath.
        '''
        print(f'{datetime.now()}: Starting process, creating db and collections')
        self.initiate_db_and_collections()
        print(f'{datetime.now()}: Created db and collections, creating relation type collection map')
        self.create_relation_type_collection_map()
        print(f'{datetime.now()}: Created relation type collection map, parsing XML')
        self.parse_xml(xml_filepath,written_form_in_sense_id, pos_in_sense_id)
        print(f'{datetime.now()}: Parsed XML, creating nodes and edges in ArangoDB')
        self.create_nodes_and_edges()
        print(f'{datetime.now()}: Nodes and edges in ArangoDB, Process Complete')

    def initiate_db_and_collections(self, sense_id_col_name='sense_ids', \
        lex_entry_col_name='lex_entries', synset_col_name='synsets', \
        syntactic_behaviour_col_name='syntactic_behaviours', edge_col_name='edges'):
            '''
            Checks if the db and collections exist, creates them if they don't.
            Associates the db and collections objects with the GraphCreator.
            '''
            self.db = self.get_db(self.connection, self.db_name)
            self.sense_id_collection = self.get_collection(self.db, sense_id_col_name)
            self.lex_entry_collection = self.get_collection(self.db, lex_entry_col_name)
            self.synset_collection = self.get_collection(self.db, synset_col_name)
            self.syntactic_behaviour_collection = self.get_collection(self.db, syntactic_behaviour_col_name)
            self.edge_collection = self.get_edge_collection(self.db, edge_col_name)
    
    def create_relation_type_collection_map(self):
        '''
        Creates a dictionary that maps relation types to their respective collections.
        ArangoDB edges start and end at "_id" field, which is "collection/_key" format.
        We need the dictionary to know which collection names to prefix to the "_key" field when creating edges.
        
        Key is the relation type, value is a tuple of the collection names that the edge will start[0] and end[1] at.
        '''
        self.relation_type_collection_map = { 
            'synset_to_synset' : (self.synset_collection.name, self.synset_collection.name),
            'sense_to_sense' : (self.sense_id_collection.name, self.sense_id_collection.name),
            'sense_to_verb_subcat' : (self.sense_id_collection.name, self.syntactic_behaviour_collection.name),
            'sense_to_lex_entry' : (self.sense_id_collection.name, self.lex_entry_collection.name),
            'sense_to_synset' : (self.sense_id_collection.name, self.synset_collection.name)
            }

    def parse_xml(self, xml_filepath:str,written_form_in_sense_id, pos_in_sense_id):
        self.xml_parser = WordNetXMLParser(xml_filepath,written_form_in_sense_id, pos_in_sense_id)
        self.xml_parser.parse()
    
    def create_nodes_and_edges(self):

        '''
        creates nodes and edges in ArangoDB from the parsed XML's dictionaries and lists.
        '''
        print(f'{datetime.now()}: adding items to sense_id_collection in ArangoDB')
        self.add_nodes_to_collection(self.sense_id_collection, self.xml_parser.sense_id_dict)
        
        print(f'{datetime.now()}: Done, adding items to lex_entry_collection in ArangoDB')
        self.add_nodes_to_collection(self.lex_entry_collection, self.xml_parser.lex_entry_dict)

        print(f'{datetime.now()}: Done, adding items to synset_collection in ArangoDB')
        self.add_nodes_to_collection(self.synset_collection, self.xml_parser.synset_dict)
        
        print(f'{datetime.now()}: Done, adding items to syntactic_behaviour_collection in ArangoDB')
        self.add_nodes_to_collection(self.syntactic_behaviour_collection, self.xml_parser.syntactic_behaviour_dict)

        print(f'{datetime.now()}: Done, adding items to edge_collection in ArangoDB')
        self.add_edges_to_collection(self.edge_collection, self.xml_parser.edge_list, self.relation_type_collection_map)

    def get_db(self,conn:Connection, db_name):
        '''
        Input connection and db_name. Creates a new DB if doesn't exist, returns the db object.
        '''
        if not conn.hasDatabase(db_name):
            conn.createDatabase(name=db_name)
        db = conn[db_name] # all databases are loaded automatically into the connection and are accessible in this fashion
        return db

    def get_collection(self,db:Database, collection_name):
        '''
        Input db and collection_name. Creates a new collection if doesn't exist, returns the collection object.
        '''
        if not db.hasCollection(collection_name):
            db.createCollection(name=collection_name) # all collections are also loaded automatically
        collection = db[collection_name]
        return collection

    def get_edge_collection(self,db:Database, collection_name):
        '''
        Input db and collection_name. Creates a new collection if doesn't exist, returns the collection object.
        '''
        if not db.hasCollection(collection_name):
            db.createCollection(name=collection_name, className='Edges') # all collections are also loaded automatically
        collection = db[collection_name]
        return collection

    def add_nodes_to_collection(self,collection:Collection, nodes_to_add):
        for key, attributes in nodes_to_add.items():
            node_doc = collection.createDocument({'_key': key})
            for attribute, value in attributes.items():
                node_doc[attribute] = value
            node_doc.save()

    def add_edges_to_collection(self,collection:Edges, edge_list, relation_type_to_collection_map):
        for edge in edge_list:
            edge_doc = collection.createDocument()
            # append the collection name to the _from and _to fields
            try:
                edge_doc['_from'] = f"{relation_type_to_collection_map[edge['relCategory']][0]}/{edge['_from']}"
                edge_doc['_to'] = f"{relation_type_to_collection_map[edge['relCategory']][1]}/{edge['_to']}"
            except KeyError:
                raise ArangoDBGraphCreator.UnexpectedRelationType(\
                    f"{edge['relCategory'][0]} / from: {edge['_from']} to: {edge['_to']}")
            edge_doc['_type'] = edge['_type']
            edge_doc.save()


def main():
    graph_creator = ArangoDBGraphCreator()
    graph_creator.create_ArangoDB_WordNet_from_XML()
    
    ## If you need to create a certain collection, uncomment the following lines and edit the final one.
    # print(f'{datetime.now()}: Starting process, creating db and collections')
    # graph_creator.initiate_db_and_collections()
    # print(f'{datetime.now()}: Created db and collections, creating relation type collection map')
    # graph_creator.create_relation_type_collection_map()
    # print(f'{datetime.now()}: Created relation type collection map, parsing XML')
    # graph_creator.parse_xml('wn.xml',True,True)
    # print(f'{datetime.now()}: Parsed XML, creating nodes and edges in ArangoDB')
    # graph_creator.add_edges_to_collection(graph_creator.edge_collection, graph_creator.xml_parser.edge_list, graph_creator.relation_type_collection_map)



if __name__ == '__main__':
    main()