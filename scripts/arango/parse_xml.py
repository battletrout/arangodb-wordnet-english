import xml.etree.ElementTree as ET

class WordNetXMLParser:
    '''
    Created by Nathan Kemp on 6 Jan, 2023 to parse the Open English WordNet wn.xml file
    into a dicts that can be used to easily put into an ArangoDB properties graph.
    May simplify working with other graph dbs such as Neo4j, too, but not tried yet.

    To compile [the .yaml into a] single [.xml] file please use the following script(s)

        python scripts/from-yaml.py
        python scripts/merge.py
    
    written_form_in_sense_id is a boolean that determines whether the written form of the word from
    the lexical entry is included in the sense ID as property (allows display of the word as node tag in ArangoDB). 
    Default True. If false, it can still be found with a graph traversal to the lex_entry.
    
    pos_in_sense_id, determines whether the part of speech is included in the sense ID. Default True.
    
    '''
    # Disallowed character list has tuples of characters that are not allowed in Arango node IDs, with their replacement.
    # used in the replace_disallowed_chars() method.
    disallowed_char_list = [('`',"'"),('ñ','n')]

    class NotXMLFileError(Exception):
        pass

    class IncorrectRelationshipError(Exception):
        pass

    class UnexpectedWordNetXMLElement(Exception):
        pass
 
    def __init__(self, xml_filepath, written_form_in_sense_id=True, pos_in_sense_id=True):
        # Check that the file is a WordNet XML file.
        self.written_form_in_sense_id = written_form_in_sense_id
        self.pos_in_sense_id = pos_in_sense_id
        
        if not xml_filepath.endswith('.xml'):
            raise WordNetXMLParser.NotXMLFileError('The file is not a XML file.')
        with open(xml_filepath, 'r') as f:
            self.tree = ET.parse(f)
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
        self.syntactic_behaviour_dict = {}

        # edge_list is a list of dicts where values are source, type (of relationship), and target.
        self.edge_list = []
    
    def replace_disallowed_chars(self, string_, disallowed_chars=disallowed_char_list):
        '''
        Takes a list of tuples; tuples are (disallowed character, replacement character).
        This is used to replace characters that are not UTF-8 for ArangoDB node IDs.
        '''
        for char_tuple in disallowed_chars:
            string_ = string_.replace(char_tuple[0],char_tuple[1])
        return string_

    def parse(self):
        '''
        Top-level wrapper that calls the other parsing functions.
        '''

        self.extract_xml_top_level()

        for child in self.root[0]:
            # Child-level tags are LexicalEntry and Synset.
            if child.tag == 'LexicalEntry':
                self.parse_lexical_entry(child)
            elif child.tag == 'Synset':
                self.parse_synset(child)
            elif child.tag == 'SyntacticBehaviour':
                self.parse_syntactic_behaviour(child)
            else:
                raise WordNetXMLParser.UnexpectedWordNetXMLElement('The tag is not a LexicalEntry or Synset.')
        
        pass
    
    def extract_xml_top_level(self):
        '''
        Extracts the top-level information from the WordNet XML file (lang, version, etc.).
        '''
        return self.root[0].attrib

    def parse_lexical_entry(self, lexical_entry):
        '''
        call when tag is lexical entry. 
        1. Creates a lexical entry node and adds it to the lex_entry_dict.
        2. Creates sense nodes and adds them to the sense_id_dict.
        3. Records edges between the sense nodes, lexical entry nodes, synsets, and other sense nodes.  
        '''      
        # Extract the lexical entry ID.
        lex_entry_id = self.replace_disallowed_chars(lexical_entry.attrib['id'])
        
        # Extract lemmas and senses. Lemmas will be properties in the current lex_entry,
        # senses will be appended to the sense dict.
        # a relationship will be added to the edge dict linking the sense to the lex_entry.
        # a relationship will be added to the edge dict linking the sense to the synset.
        for child in lexical_entry:
            if child.tag == 'Lemma':
                lex_written_form = str(child.attrib['writtenForm'])
                lex_pos = str(child.attrib['partOfSpeech'])
                # add the lemma to the lex_entry_dict.
                # also add the id as a specific
                self.lex_entry_dict[lex_entry_id] = {'writtenForm' : lex_written_form, 'partOfSpeech' : lex_pos}
                # Add pronunciation to lex_entry_dict if specified.
                for grandchild in child:
                    if grandchild.tag == 'Pronunciation':
                        self.lex_entry_dict[lex_entry_id]['Pronunciation'] = grandchild.text
                    #print(self.lex_entry_dict[lex_entry_id]['Pronunciation'])
            
            elif child.tag == 'Sense':
                sense_id = self.replace_disallowed_chars(child.attrib['id'])
                # add the sense to the sense dict
                self.sense_id_dict[sense_id] = child.attrib #ensure all attributes are included.
                # add written form and pos to sense_id_dict if specified.
                if self.written_form_in_sense_id: self.sense_id_dict[sense_id]['writtenForm'] = lex_written_form
                if self.pos_in_sense_id: self.sense_id_dict[sense_id]['partOfSpeech'] = lex_pos

                # add the sense to synset to the edge dict.
                self.add_edge(sense_id, child.attrib['synset'], 'synset_member_of', 'sense_to_synset') 

                # add the sense to lexical_entry relationship to the edge dict.
                self.add_edge(sense_id, lex_entry_id, 'lex_member_of', 'sense_to_lex_entry')

                # add the sense to verb subcat relationship to the edge dict, if specified.
                # 
                if 'subcat' in child.attrib:
                    for split_subcat in child.attrib['subcat'].split(' '):
                        self.add_edge(sense_id, split_subcat, 'verb_subcat_of', 'sense_to_verb_subcat')

                # check if the tag has any SenseRelationships or verb subcategories.
                # if so, add to the edge dict.
                for grandchild in child:
                    if grandchild.tag == 'SenseRelation':
                        # add the relationship to the edge dict.
                        # edge_dict key is source, value is a dict with keys type (of relationship) and target.
                        self.add_edge(sense_id, grandchild.attrib['target'], grandchild.attrib['relType'],'sense_to_sense')
                        # print(sense_id, grandchild.attrib['target'], grandchild.attrib['relType'])
                        # print(self.edge_list[-1])

            elif child.tag == 'Form':
                self.lex_entry_dict[lex_entry_id] = {**self.lex_entry_dict[lex_entry_id],**child.attrib}
                # print(self.lex_entry_dict[lex_entry_id])

            else:
                raise WordNetXMLParser.UnexpectedWordNetXMLElement('Unexpected tag in lexical entry: {}'.format(child.tag))


    def parse_synset(self, synset):
        '''
        call when tag is synset.
        1. Creates a synset node and adds it to the synset_dict.
        2. Creates entries in the edge_list for the synsets' relationships.
        '''
        # Extract the synset ID.
        synset_id = self.replace_disallowed_chars(synset.attrib['id'])
        self.synset_dict[synset_id] = synset.attrib

        # Create a list for holding this synset's examples (to allow multiple examples for each synset).
        self.synset_dict[synset_id]['Examples'] = []

        for child in synset:
            if child.tag == 'Definition':
                # add the def to the synset_dict.
                self.synset_dict[synset_id]['Definition'] = child.text

            elif child.tag == 'ILIDefinition':
                # add the ILIdef to the synset_dict.
                self.synset_dict[synset_id]['ILIDefinition'] = child.text

            elif child.tag == 'SynsetRelation':
                # add the relationship to the edge dict.
                self.add_edge(synset_id, child.attrib['target'], child.attrib['relType'],'synset_to_synset')
                # print(synset_id, child.attrib['target'], child.attrib['relType'])
                # print(self.edge_list[-1])            
            
            elif child.tag == 'Example':
                # add the example to the synset_dict['Examples'] list.
                self.synset_dict[synset_id]['Examples'].append(child.text)

            else:
                raise WordNetXMLParser.UnexpectedWordNetXMLElement('Unexpected tag in synset entry: {}'.format(child.tag))    

    def parse_syntactic_behaviour(self, syntactic_behaviour):
        '''
        call when tag is syntacticBehaviour.
        Creates a syntacticBehaviour node and adds it to the syntactic_behaviour_dict.
        '''
        # Extract the syntactic_behaviour ID.
        syntactic_behaviour_id = syntactic_behaviour.attrib['id']
        self.syntactic_behaviour_dict[syntactic_behaviour_id] = { \
            'subcategorizationFrame' : syntactic_behaviour.attrib['subcategorizationFrame']}

    def add_edge(self, source, target, relType, relCategory):
        # Add the edge to the edge list. Format is a dict with keys _from, _to, and _type, necessary for arango import.
        # RelCategory is used when making the edge collection to append the collection name with the key.
        self.edge_list.append({'relCategory': relCategory, '_from': source, '_to': self.replace_disallowed_chars(target), '_type': relType})

    def print_all(self):
        # for debugging
        print(self.wordnet_set_info)
        print()
        print(self.lex_entry_dict)
        print()
        print(self.sense_id_dict)
        print()
        print(self.synset_dict)
        print()
        print(self.syntactic_behaviour_dict)
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

