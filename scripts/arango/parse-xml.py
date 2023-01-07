import xml.etree.ElementTree as ET

class NotXMLFileError(Exception):
    pass

class IncorrectRelationshipError(Exception):
    pass

class WordNetXMLParser:
    '''
    Created by Nathan Kemp on 6 Jan, 2023 to parse the Open English WordNet wn.xml file
    into a dicts that can be used to easily put into an ArangoDB properties graph.
    May simplify working with other graph dbs such as Neo4j, too, but not tried yet.

    NOTE: Pronunciation is dropped, as it is UTF-16 and not supported in ArangoDB queries.

    To compile [the .yaml into a] single [.xml] file please use the following script(s)

        python scripts/from-yaml.py
        python scripts/merge.py
    
    written_form_in_sense_id is a boolean that determines whether the written form of the word from
    the lexical entry is included in the sense ID as property (allows display of the word as node tag in ArangoDB). 
    Default True. If false, it can still be found with a graph traversal to the lex_entry.
    
    pos_in_sense_id, determines whether the part of speech is included in the sense ID. Default True.
    
    '''
 
    def __init__(self, xml_file, written_form_in_sense_id=True, pos_in_sense_id=True):
        # Check that the file is a WordNet XML file.
        self.written_form_in_sense_id = written_form_in_sense_id
        self.pos_in_sense_id = pos_in_sense_id
        
        if not xml_file.endswith('.xml'):
            raise NotXMLFileError('The file is not a XML file.')
        with open(xml_file, 'r') as f:
            self.tree = ET.parse(xml_file)
        self.root = self.tree.getroot()

        # The set info dict will store the WordNet set information, such as the /
        # language, version, etc.
        self.wordnet_set_info = self.extract_xml_top_level()
        
        # Declare the dicts that will store nodes and edges: sense IDs, lexical entries, and synsets.
        # 1. Sense IDs are the unique IDs for each word sense, and are used to link to the synsets.
        # 2. Lexical entries are the unique IDs for each word, and are used to link to the sense IDs.
        # 3. Synsets link similar words, and maintain the relationships between word groups \
        #    such as hyponyms and hypernyms.
        # 4. Edges will include all relationships between nodes; synsets have multiple types \
        #    of relationships, Sense IDs may have multiple types of SenseRelationships (or none), \
        #    and Lexical Entries will have Sense IDs that are members of them.
        
        
        self.sense_id_dict = {}
        self.lex_entry_dict = {}
        self.synset_dict = {}

        # edge_list is a list of dicts where values are source, type (of relationship), and target.
        self.edge_list = []
    
    def parse(self):
        self.extract_xml_top_level()

        for child in self.root[0][0:5005]:
            # Child-level tags are LexicalEntry and Synset.
            if child.tag == 'LexicalEntry':
                self.parse_lexical_entry(child)
            # print(child.tag, child.attrib)
            
            
            
            # for grandchild in child:
            #     print(grandchild.tag, grandchild.attrib)
            #     for grandgrandchild in grandchild:
            #         print(grandgrandchild.tag, grandgrandchild.attrib)
            # print()
    


    # Extract the WordNet header information (lang, version, etc.)
    def extract_xml_top_level(self):
        return self.root[0].attrib

    def parse_lexical_entry(self, lexical_entry):
        
        # Extract the lexical entry ID.
        lex_entry_id = lexical_entry.attrib['id']
        
        # Extract lemmas and senses. Lemmas will be properties in the current lex_entry,
        # senses will be appended to the sense dict.
        # a relationship will be added to the edge dict linking the sense to the lex_entry.
        # a relationship will be added to the edge dict linking the sense to the synset.
        for child in lexical_entry:
            if child.tag == 'Lemma':
                lex_written_form = str(child.attrib['writtenForm'])
                lex_pos = str(child.attrib['partOfSpeech'])
                # add the lemma to the lex_entry_dict.
                self.lex_entry_dict[lex_entry_id] = {'writtenForm' : lex_written_form, 'partOfSpeech' : lex_pos}
            
            
            elif child.tag == 'Sense':
                sense_id = child.attrib['id']
                # add the sense to the sense dict
                self.sense_id_dict[sense_id] = child.attrib #ensure all attributes are included.
                # self.sense_id_dict[sense_id].pop('synset') #remove synset, as that's a relationship.
                # add written form and pos to sense_id_dict if specified.
                if self.written_form_in_sense_id: self.sense_id_dict[sense_id]['writtenForm'] = lex_written_form
                if self.pos_in_sense_id: self.sense_id_dict[sense_id]['partOfSpeech'] = lex_pos

                # add the sense to synset to the edge dict.
                self.add_edge(sense_id, child.attrib['synset'], 'synset_member_of') 

                # add the sense to lexical_entry relationship to the edge dict.
                self.add_edge(sense_id, lex_entry_id, 'lex_member_of')

                # check if the tag has any SenseRelationships
                # if so, add to the edge dict.
                for grandchild in child:
                    if grandchild.tag == 'SenseRelation':
                        # add the relationship to the edge dict.
                        # edge_dict key is source, value is a dict with keys type (of relationship) and target.
                        self.add_edge(sense_id, grandchild.attrib['target'], grandchild.attrib['relType'])
                        print(sense_id, grandchild.attrib['target'], grandchild.attrib['relType'])
                        print(self.edge_list[-1])

            elif child.tag == 'Form':
                self.lex_entry_dict[lex_entry_id] = {**self.lex_entry_dict[lex_entry_id],**child.attrib}
                print(self.lex_entry_dict[lex_entry_id])

            else:
                raise ValueError('Unexpected tag in lexical entry: {}'.format(child.tag))


    def add_sense_ids(self, lexical_entry):
        # Senses of words are stored in a dict with the sense ID as the key.
        # Each sense ID has a dict of its own, which will store the word, POS, and synset ID.
        
        self.sense_id_dict[sense_id] = {}

    def add_edge(self, source, target, relType):
        self.edge_list.append({'source': source, 'target': target, 'relType': relType})

    def print_all(self):
        print(self.wordnet_set_info)
        print()
        print(self.lex_entry_dict)
        print()
        print(self.sense_id_dict)
        print()
        print(self.edge_list)
        print()

def main():
    parser = WordNetXMLParser('wn.xml')
    parser.parse()
    # print(WordNetXMLParser.__doc__)
    parser.print_all()



if __name__ == '__main__':
    main()

# print(root[0][0].items())
# print(root[0][0][0].tag, root[0][0][0].attrib)
