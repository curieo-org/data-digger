MATCH
    p=(similar_compound:compound_details)<-[similarity:similar_to]-(compound:compound_details)<-[parent_edge:has_parent]-(input)
WHERE
    id(input) == 'aspirin'
RETURN
    similar_compound.compound_details.molecule_name as similar_compound_name,
    similar_compound.compound_details.canonical_smiles as canonical_smiles,
    similarity.similarity as percentage_of_similarity;

MATCH
    p=(drug_ind:drug_indication)<-[drug_ind_edge:compound_of]-(compound:compound_details)<-[]-(input)
WHERE
    id(input) == 'aspirin'
RETURN
    drug_ind.drug_indication.mesh_id as mesh_id,
    drug_ind.drug_indication.mesh_heading as mesh_heading,
    drug_ind.drug_indication.max_phase_for_ind as max_phase_for_ind,
    drug_ind.drug_indication.reference_url as reference_url,
    drug_ind.drug_indication.source_description as source_description;


MATCH
    p=(target_compound:target_details)-[tar_edge:target_of]->(drug_mec:drug_mechanism)<-[drug_mec_edge:compound_of]-(compound:compound_details)<-[]-(input)
WHERE
    id(input) == 'aspirin'
RETURN
    drug_mec.drug_mechanism.action_type as action_type,
    drug_mec.drug_mechanism.mechanism_of_action as mechanism_of_action,
    drug_mec.drug_mechanism.reference_url as reference_url,
    drug_mec.drug_mechanism.source_description as source_description,
    target_compound.target_details.target_type as target_type,
    target_compound.target_details.target_name as target_name,
    target_compound.target_details.organism as organism;


MATCH
    p=(synonym:molecule_synonym)-[syn_edge:synonym_of]->(compound:compound_details)<-[parent_edge:has_parent]-(input)
WHERE
    id(input) == 'aspirin'
RETURN
    synonym.molecule_synonym.value;

MATCH
    p=(compound:compound_details)<-[parent_edge:has_parent]-(input)
WHERE
    id(input) == 'aspirin'
RETURN
    compound.compound_details.molecule_name as molecule_name,
    compound.compound_details.chembl_id as chembl_id,
    compound.compound_details.molecule_type as molecule_type,
    compound.compound_details.first_approval as first_approval,
    compound.compound_details.max_phase as max_phase,
    compound.compound_details.full_weight as full_weight,
    compound.compound_details.monoisotopic_weight as monoisotopic_weight,
    compound.compound_details.parent_compound_weight as parent_compound_weight,
    compound.compound_details.alogp as alogp,
    compound.compound_details.rotatable_bonds as rotatable_bonds,
    compound.compound_details.polar_surface_area as polar_surface_area,
    compound.compound_details.molecular_species as molecular_species,
    compound.compound_details.hba as hba,
    compound.compound_details.hbd as hbd,
    compound.compound_details.moleculero5_violations_name as ro5_violations,
    compound.compound_details.hba_lipinski as hba_lipinski,
    compound.compound_details.hbd_lipinski as hbd_lipinski,
    compound.compound_details.ro5_violations_lipinski as ro5_violations_lipinski,
    compound.compound_details.cx_acidic_pka as cx_acidic_pka,
    compound.compound_details.cx_basic_pka as cx_basic_pka,
    compound.compound_details.cx_logp as cx_logp,
    compound.compound_details.cx_logd as cx_logd,
    compound.compound_details.aromatic_rings as aromatic_rings,
    compound.compound_details.heavy_atoms as heavy_atoms,
    compound.compound_details.qed_weighted as qed_weighted,
    compound.compound_details.np_likeness_score as np_likeness_score,
    compound.compound_details.full_molecular_formula as full_molecular_formula,
    compound.compound_details.standard_inchi as standard_inchi,
    compound.compound_details.standard_inchi_key as standard_inchi_key,
    compound.compound_details.canonical_smiles as canonical_smiles;