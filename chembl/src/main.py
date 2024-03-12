from chembl_query import ChemblQuery
from graphdb import GraphDB
from chembl_similarity import ChemblSimilarity
from uuid import uuid4
from flask import Flask
import os

BACKEND_HOST = os.getenv('BACKEND_HOST', '')
BACKEND_PORT = os.getenv('BACKEND_PORT', '')
app = Flask(__name__)


# Create the necessary graph properties
def create_necessary_graph_properties():
    graphdb = GraphDB()

    # Necessary vertexes tags and properties
    tag_list = {
        # Main Vertexes
        'compound_details': [
            ('molecular_uid', 'string'),
            ('molecule_name', 'string'),
            ('chembl_id', 'string'),
            ('molecule_type', 'string'),
            ('first_approval', 'string'),
            ('max_phase', 'string'),
            ('full_weight', 'string'),
            ('monoisotopic_weight', 'string'),
            ('parent_compound_weight', 'string'),
            ('alogp', 'string'),
            ('rotatable_bonds', 'string'),
            ('polar_surface_area', 'string'),
            ('molecular_species', 'string'),
            ('hba', 'string'),
            ('hbd', 'string'),
            ('ro5_violations', 'string'),
            ('hba_lipinski', 'string'),
            ('hbd_lipinski', 'string'),
            ('ro5_violations_lipinski', 'string'),
            ('cx_acidic_pka', 'string'),
            ('cx_basic_pka', 'string'),
            ('cx_logp', 'string'),
            ('cx_logd', 'string'),
            ('aromatic_rings', 'string'),
            ('heavy_atoms', 'string'),
            ('qed_weighted', 'string'),
            ('np_likeness_score', 'string'),
            ('full_molecular_formula', 'string'),
            ('standard_inchi', 'string'),
            ('standard_inchi_key', 'string'),
            ('canonical_smiles', 'string'),
        ],
        'target_details': [
            ('target_uid', 'string'),
            ('target_type', 'string'),
            ('target_name', 'string'),
            ('organism', 'string'),
            ('chembl_id', 'string'),
        ],
        'drug_mechanism': [
            ('molecular_uid', 'string'),
            ('action_type', 'string'),
            ('mechanism_of_action', 'string'),
            ('target_uid', 'string'),
            ('reference_type', 'string'),
            ('reference_id', 'string'),
            ('reference_url', 'string'),
            ('compound_name', 'string'),
            ('compound_key', 'string'),
            ('source_name', 'string'),
            ('source_description', 'string'),
        ],
        'drug_indication': [
            ('molecular_uid', 'string'),
            ('mesh_id', 'string'),
            ('mesh_heading', 'string'),
            ('max_phase_for_ind', 'string'),
            ('reference_type', 'string'),
            ('reference_id', 'string'),
            ('reference_url', 'string'),
            ('compound_name', 'string'),
            ('compound_key', 'string'),
            ('source_name', 'string'),
            ('source_description', 'string'),
        ],
        # 'assay_details',
        # 'document_details',
        # 'cell_details',
        # 'tissue_details',

        # Query Vertexes
        'molecule_name': [ ('value', 'string') ],
        'chembl_id': [ ('value', 'string') ],
        'molecule_type': [ ('value', 'string') ],
        'full_molecular_formula': [ ('value', 'string') ],
        'molecule_synonym': [ ('value', 'string') ],
        'action_type': [ ('value', 'string') ],
        'mechanism_of_action': [ ('value', 'string') ],
        'reference_type': [ ('value', 'string') ],
        'mesh_heading': [ ('value', 'string') ],
        'compound_name': [ ('value', 'string') ],
        'compound_key': [ ('value', 'string') ],
        'source_name': [ ('value', 'string') ],
        'source_description': [ ('value', 'string') ],
        'target_uid': [ ('value', 'string') ],
        'target_type': [ ('value', 'string') ],
        'target_name': [ ('value', 'string') ],
        'organism': [ ('value', 'string') ],
    }

    # Necessary edges tags and properties
    edge_list = {
        'has_parent': [ ('value', 'string') ],
        'synonym_of': [ ('value', 'string') ],
        'target_of': [ ('value', 'string') ],
        'compound_of': [ ('value', 'string') ],
        'similar_to': [ ('value', 'string'), ('similarity', 'string'), ('similarity_key', 'string') ],
    }

    # Creating the vertexes in the graph
    for tag in tag_list.keys():
        tag_name = tag
        properties = dict()
        for property in tag_list[tag]:
            properties[property[0]] = property[1]

        graphdb.create_tag(tag_name, properties)

    # Creating the edges in the graph
    for edge in edge_list.keys():
        edge_name = edge
        properties = dict()
        for property in edge_list[edge]:
            properties[property[0]] = property[1]

        graphdb.create_edge(edge_name, properties)

    graphdb.close()


# Populate the compound info
def populate_compound_info():
    result = ChemblQuery.get_compound_info()
    graphdb = GraphDB()
    print('Total compounds:', len(result))

    for compound in result:
        # Check data integrity
        if compound['molecular_uid'] == '':
            continue
        
        # Extracting the values from the dictionary
        molecular_uid = compound['molecular_uid'].lower() + '_compound'
        molecule_name = compound['molecule_name'].lower()
        chembl_id = compound['chembl_id'].lower()
        molecule_type = compound['molecule_type'].lower()
        full_molecular_formula = compound['full_molecular_formula'].lower()
        

        # Inserting the root vertex
        graphdb.insert_vertex('compound_details', molecular_uid, compound)

        # Inserting the child vertexes and edges
        if molecule_name != '':
            graphdb.insert_vertex('molecule_name', molecule_name, {
                'value': molecule_name
            })
            graphdb.insert_edge('has_parent', molecule_name, molecular_uid, {
                'value': 'has_parent'
            })
        
        if chembl_id != '':
            graphdb.insert_vertex('chembl_id', chembl_id, {
                'value': chembl_id
            })
            graphdb.insert_edge('has_parent', chembl_id, molecular_uid, {
                'value': 'has_parent'
            })

        if molecule_type != '':
            graphdb.insert_vertex('molecule_type', molecule_type, {
                'value': molecule_type
            })
            graphdb.insert_edge('has_parent', molecule_type, molecular_uid, {
                'value': 'has_parent'
            })

        if full_molecular_formula != '':
            graphdb.insert_vertex('full_molecular_formula', full_molecular_formula, {
                'value': compound['full_molecular_formula']
            })
            graphdb.insert_edge('has_parent', full_molecular_formula, molecular_uid, {
                'value': 'has_parent'
            })

    graphdb.close()



# Populate the target info
def populate_target_info():
    result = ChemblQuery.get_target_info()
    graphdb = GraphDB()
    print('Total targets:', len(result))

    for target in result:
        # Check data integrity
        if target['target_uid'] == '':
            continue

        # Extracting the values from the dictionary
        target_uid = target['target_uid'].lower() + '_target'
        target_type = target['target_type'].lower()
        target_name = target['target_name'].lower()
        organism = target['organism'].lower()
        chembl_id = target['chembl_id'].lower()

        # Inserting the root vertex
        graphdb.insert_vertex('target_details', target_uid, target)

        # Inserting the child vertexes and edges
        if target_type != '':
            graphdb.insert_vertex('target_type', target_type, {
                'value': target_type
            })
            graphdb.insert_edge('has_parent', target_type, target_uid, {
                'value': 'has_parent'
            })

        if target_name != '':
            graphdb.insert_vertex('target_name', target_name, {
                'value': target_name
            })
            graphdb.insert_edge('has_parent', target_name, target_uid, {
                'value': 'has_parent'
            })

        if organism != '':
            graphdb.insert_vertex('organism', organism, {
                'value': organism
            })
            graphdb.insert_edge('has_parent', organism, target_uid, {
                'value': 'has_parent'
            })

        if chembl_id != '':
            graphdb.insert_vertex('chembl_id', chembl_id, {
                'value': chembl_id
            })
            graphdb.insert_edge('has_parent', chembl_id, target_uid, {
                'value': 'has_parent'
            })

    graphdb.close()


# Populate the compound similarity
def populate_compound_similarity():
    result = ChemblQuery.get_compound_info()
    graphdb = GraphDB()
    chembl_similarity = ChemblSimilarity()
    print('Total compounds:', len(result))

    for compound in result:
        # Check data integrity
        if compound['molecular_uid'] == '' or compound['canonical_smiles'] == '':
            continue
        
        # Extracting the values from the dictionary
        molecular_uid = compound['molecular_uid'].lower() + '_compound'
        canonical_smiles = compound['canonical_smiles']

        # Find similar compounds
        try:
            similar_compounds = chembl_similarity.compound_similarity(canonical_smiles)


            # Inserting the child vertexes and edges
            for similar_compound in similar_compounds:
                molregno = similar_compound['molregno'] + '_compound'
                similarity = similar_compound['similarity']
                similarity_key = canonical_smiles

                graphdb.insert_edge('similar_to', molecular_uid, molregno, {
                    'value': 'similar_to',
                    'similarity': similarity,
                    'similarity_key': similarity_key
                })
        except:
            print('Error in finding similar compounds for:', molecular_uid, canonical_smiles)

    graphdb.close()



# Populate the compound synonyms
def populate_compound_synonyms():
    result = ChemblQuery.get_compound_synonyms()
    graphdb = GraphDB()
    print('Total synonyms:', len(result))

    for compound in result:
        # Check data integrity
        if compound['molecular_uid'] == '':
            continue

        # Extracting the values from the dictionary
        molecular_uid = compound['molecular_uid'].lower() + '_compound'
        molecule_synonym = compound['molecule_synonym'].lower()

        # Inserting the child vertexes and edges
        if molecule_synonym != '':
            graphdb.insert_vertex('molecule_synonym', molecule_synonym, {
                'value': molecule_synonym
            })
            graphdb.insert_edge('synonym_of', molecule_synonym, molecular_uid, {
                'value': 'synonym_of'
            })

    graphdb.close()



# Populate the drug mechanism
def populate_drug_mechanism():
    result = ChemblQuery.get_drug_mechanism_info()
    graphdb = GraphDB()
    print('Total drug mechanisms:', len(result))

    for drug in result:
        # Check data integrity
        if drug['molecular_uid'] == '':
            continue

        # Extracting the values from the dictionary
        molecular_uid = drug['molecular_uid'].lower() + '_compound'
        action_type = drug['action_type'].lower()
        mechanism_of_action = drug['mechanism_of_action'].lower()
        target_uid = drug['target_uid'].lower() + '_target'
        reference_type = drug['reference_type'].lower()
        compound_name = drug['compound_name'].lower()
        compound_key = drug['compound_key'].lower()
        source_name = drug['source_name'].lower()
        source_description = drug['source_description'].lower()

        # Inserting the root vertex
        new_uuid = str(uuid4())
        graphdb.insert_vertex('drug_mechanism', new_uuid, drug)

        # Inserting the child vertexes and edges
        if molecular_uid != '':
            graphdb.insert_edge('compound_of', molecular_uid, new_uuid, {
                'value': 'compound_of'
            })

        if action_type != '':
            graphdb.insert_vertex('action_type', action_type, {
                'value': action_type
            })
            graphdb.insert_edge('has_parent', action_type, new_uuid, {
                'value': 'has_parent'
            })

        if mechanism_of_action != '':
            graphdb.insert_vertex('mechanism_of_action', mechanism_of_action, {
                'value': mechanism_of_action
            })
            graphdb.insert_edge('has_parent', mechanism_of_action, new_uuid, {
                'value': 'has_parent'
            })

        if target_uid != '':
            graphdb.insert_edge('target_of', target_uid, new_uuid, {
                'value': 'target_of'
            })

        if reference_type != '':
            graphdb.insert_vertex('reference_type', reference_type, {
                'value': reference_type
            })
            graphdb.insert_edge('has_parent', reference_type, new_uuid, {
                'value': 'has_parent'
            })

        if compound_name != '':
            graphdb.insert_vertex('compound_name', compound_name, {
                'value': compound_name
            })
            graphdb.insert_edge('has_parent', compound_name, new_uuid, {
                'value': 'has_parent'
            })

        if compound_key != '':
            graphdb.insert_vertex('compound_key', compound_key, {
                'value': compound_key
            })
            graphdb.insert_edge('has_parent', compound_key, new_uuid, {
                'value': 'has_parent'
            })

        if source_name != '':
            graphdb.insert_vertex('source_name', source_name, {
                'value': source_name
            })
            graphdb.insert_edge('has_parent', source_name, new_uuid, {
                'value': 'has_parent'
            })

        if source_description != '':
            graphdb.insert_vertex('source_description', source_description, {
                'value': source_description
            })
            graphdb.insert_edge('has_parent', source_description, new_uuid, {
                'value': 'has_parent'
            })

    graphdb.close()


# Populate the drug indication
def populate_drug_indication():
    result = ChemblQuery.get_drug_indication_info()
    graphdb = GraphDB()
    print('Total drug indications:', len(result))

    for drug in result:
        # Check data integrity
        if drug['molecular_uid'] == '':
            continue

        # Extracting the values from the dictionary
        molecular_uid = drug['molecular_uid'].lower() + '_compound'
        mesh_heading = drug['mesh_heading'].lower()
        reference_type = drug['reference_type'].lower()
        compound_name = drug['compound_name'].lower()
        compound_key = drug['compound_key'].lower()
        source_name = drug['source_name'].lower()
        source_description = drug['source_description'].lower()

        # Inserting the root vertex
        new_uuid = str(uuid4())
        graphdb.insert_vertex('drug_indication', new_uuid, drug)

        # Inserting the child vertexes and edges
        if molecular_uid != '':
            graphdb.insert_edge('compound_of', molecular_uid, new_uuid, {
                'value': 'compound_of'
            })

        if mesh_heading != '':
            graphdb.insert_vertex('mesh_heading', mesh_heading, {
                'value': mesh_heading
            })
            graphdb.insert_edge('has_parent', mesh_heading, new_uuid, {
                'value': 'has_parent'
            })

        if reference_type != '':
            graphdb.insert_vertex('reference_type', reference_type, {
                'value': reference_type
            })
            graphdb.insert_edge('has_parent', reference_type, new_uuid, {
                'value': 'has_parent'
            })

        if compound_name != '':
            graphdb.insert_vertex('compound_name', compound_name, {
                'value': compound_name
            })
            graphdb.insert_edge('has_parent', compound_name, new_uuid, {
                'value': 'has_parent'
            })

        if compound_key != '':
            graphdb.insert_vertex('compound_key', compound_key, {
                'value': compound_key
            })
            graphdb.insert_edge('has_parent', compound_key, new_uuid, {
                'value': 'has_parent'
            })

        if source_name != '':
            graphdb.insert_vertex('source_name', source_name, {
                'value': source_name
            })
            graphdb.insert_edge('has_parent', source_name, new_uuid, {
                'value': 'has_parent'
            })

        if source_description != '':
            graphdb.insert_vertex('source_description', source_description, {
                'value': source_description
            })
            graphdb.insert_edge('has_parent', source_description, new_uuid, {
                'value': 'has_parent'
            })

    graphdb.close()


def main():
    create_necessary_graph_properties()
    populate_compound_info()
    populate_target_info()
    populate_compound_similarity()
    populate_compound_synonyms()
    populate_drug_mechanism()
    populate_drug_indication()


@app.route('/start-transfer-database')
def start_transfer_database():
    main()

if __name__ == "__main__":
    app.run(host=BACKEND_HOST, port=BACKEND_PORT)