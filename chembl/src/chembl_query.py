import psycopg2


def connect() -> psycopg2._psycopg.connection:
    connection = psycopg2.connect(
        database='chembl33',
        user='rathijitpaul',
        host='localhost',
    )

    return connection



class ChemblQuery:

    @staticmethod
    def get_single_compound_info(filter_value: str, filter_type: str) -> dict[str, str]:
        connection = connect()
        cursor = connection.cursor()

        query = '''
            select
                mol_dic.molregno as molecular_uid,
                mol_dic.pref_name as molecule_name,
                mol_dic.chembl_id as chembl_id,
                mol_dic.molecule_type as molecule_type,
                com_pro.full_mwt as full_weight,
                com_pro.mw_monoisotopic as monoisotopic_weight,
                com_pro.mw_freebase as parent_compound_weight,
                com_pro.alogp as alogp,
                com_pro.rtb as rotatable_bonds,
                com_pro.psa as polar_surface_area,
                com_pro.molecular_species as molecular_species,
                com_pro.hba as hba,
                com_pro.hbd as hbd,
                com_pro.num_ro5_violations as ro5_violations,
                com_pro.hba_lipinski as hba_lipinski,
                com_pro.hbd_lipinski as hbd_lipinski,
                com_pro.num_lipinski_ro5_violations as ro5_violations_lipinski,
                com_pro.cx_most_apka as cx_acidic_pka,
                com_pro.cx_most_bpka as cx_basic_pka,
                com_pro.cx_logp as cx_logp,
                com_pro.cx_logd as cx_logd,
                com_pro.aromatic_rings as aromatic_rings,
                com_pro.heavy_atoms as heavy_atoms,
                com_pro.qed_weighted as qed_weighted,
                com_pro.np_likeness_score as np_likeness_score,
                com_pro.full_molformula as full_molecular_formula,
                com_struc.standard_inchi as standard_inchi,
                com_struc.standard_inchi_key as standard_inchi_key,
                com_struc.canonical_smiles as canonical_smiles
            from
                public.molecule_dictionary as mol_dic full join
                public.compound_properties as com_pro on mol_dic.molregno = com_pro.molregno full join
                public.compound_structures as com_struc on mol_dic.molregno = com_struc.molregno
            where
        '''

        if filter_type == 'molregno':
            query += f'''
                mol_dic.molregno = {filter_value};
            '''
        elif filter_type == 'chembl_id':
            query += f'''
                mol_dic.chembl_id = '{filter_value}';
            '''
        elif filter_type == 'molecule_name':
            query += f'''
                mol_dic.pref_name = '{filter_value}';
            '''
        else:
            raise ValueError('Invalid filter type')

        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        data_dictionary = []

        for row in result:
            row = list(row)
            for index, value in enumerate(row):
                row[index] = str(value) if value is not None else ''
                
            molecular_uid, molecule_name, chembl_id, molecule_type, full_weight, monoisotopic_weight, parent_compound_weight, alogp, rotatable_bonds, polar_surface_area, molecular_species, hba, hbd, ro5_violations, hba_lipinski, hbd_lipinski, ro5_violations_lipinski, cx_acidic_pka, cx_basic_pka, cx_logp, cx_logd, aromatic_rings, heavy_atoms, qed_weighted, np_likeness_score, full_molecular_formula, standard_inchi, standard_inchi_key, canonical_smiles = row

            data_dictionary.append({
                'molecular_uid': molecular_uid,
                'molecule_name': molecule_name,
                'chembl_id': chembl_id,
                'molecule_type': molecule_type,
                'full_weight': full_weight,
                'monoisotopic_weight': monoisotopic_weight,
                'parent_compound_weight': parent_compound_weight,
                'alogp': alogp,
                'rotatable_bonds': rotatable_bonds,
                'polar_surface_area': polar_surface_area,
                'molecular_species': molecular_species,
                'hba': hba,
                'hbd': hbd,
                'ro5_violations': ro5_violations,
                'hba_lipinski': hba_lipinski,
                'hbd_lipinski': hbd_lipinski,
                'ro5_violations_lipinski': ro5_violations_lipinski,
                'cx_acidic_pka': cx_acidic_pka,
                'cx_basic_pka': cx_basic_pka,
                'cx_logp': cx_logp,
                'cx_logd': cx_logd,
                'aromatic_rings': aromatic_rings,
                'heavy_atoms': heavy_atoms,
                'qed_weighted': qed_weighted,
                'np_likeness_score': np_likeness_score,
                'full_molecular_formula': full_molecular_formula,
                'standard_inchi': standard_inchi,
                'standard_inchi_key': standard_inchi_key,
                'canonical_smiles': canonical_smiles
            })

        return data_dictionary[0]
        

    @staticmethod
    def get_target_info() -> list[dict[str, str]]:
        connection = connect()
        cursor = connection.cursor()

        query = '''
            select
                tar_dic.tid as target_uid,
                tar_dic.target_type as target_type,
                tar_dic.pref_name as target_name,
                tar_dic.organism as organism,
                tar_dic.chembl_id as chembl_id
            from
                public.target_dictionary as tar_dic;
        '''

        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        data_dictionary = []

        for row in result:
            row = list(row)
            for index, value in enumerate(row):
                row[index] = str(value) if value is not None else ''

            target_uid, target_type, target_name, organism, chembl_id = row

            data_dictionary.append({
                'target_uid': target_uid,
                'target_type': target_type,
                'target_name': target_name,
                'organism': organism,
                'chembl_id': chembl_id
            })

        return data_dictionary
    

    @staticmethod
    def get_compound_info() -> list[dict[str, str]]:
        connection = connect()
        cursor = connection.cursor()

        query = '''
            select
                mol_dic.molregno as molecular_uid,
                mol_dic.pref_name as molecule_name,
                mol_dic.chembl_id as chembl_id,
                mol_dic.molecule_type as molecule_type,
                mol_dic.first_approval as first_approval,
                case
                    when mol_dic.max_phase is null then 'Preclinical Compound'
                    when mol_dic.max_phase = -1 then 'Clinical Phase Unknown'
                    when mol_dic.max_phase = 0.5 then 'Early Phase 1 Clinical Trials'
                    when mol_dic.max_phase = 1 then 'Phase 1 Clinical Trials'
                    when mol_dic.max_phase = 2 then 'Phase 2 Clinical Trials'
                    when mol_dic.max_phase = 3 then 'Phase 3 Clinical Trials'
                    when mol_dic.max_phase = 4 then 'Approved'
                end
                as max_phase,
                com_pro.full_mwt as full_weight,
                com_pro.mw_monoisotopic as monoisotopic_weight,
                com_pro.mw_freebase as parent_compound_weight,
                com_pro.alogp as alogp,
                com_pro.rtb as rotatable_bonds,
                com_pro.psa as polar_surface_area,
                com_pro.molecular_species as molecular_species,
                com_pro.hba as hba,
                com_pro.hbd as hbd,
                com_pro.num_ro5_violations as ro5_violations,
                com_pro.hba_lipinski as hba_lipinski,
                com_pro.hbd_lipinski as hbd_lipinski,
                com_pro.num_lipinski_ro5_violations as ro5_violations_lipinski,
                com_pro.cx_most_apka as cx_acidic_pka,
                com_pro.cx_most_bpka as cx_basic_pka,
                com_pro.cx_logp as cx_logp,
                com_pro.cx_logd as cx_logd,
                com_pro.aromatic_rings as aromatic_rings,
                com_pro.heavy_atoms as heavy_atoms,
                com_pro.qed_weighted as qed_weighted,
                com_pro.np_likeness_score as np_likeness_score,
                com_pro.full_molformula as full_molecular_formula,
                com_struc.standard_inchi as standard_inchi,
                com_struc.standard_inchi_key as standard_inchi_key,
                com_struc.canonical_smiles as canonical_smiles
            from
                public.molecule_dictionary as mol_dic full join
                public.compound_properties as com_pro on mol_dic.molregno = com_pro.molregno full join
                public.compound_structures as com_struc on mol_dic.molregno = com_struc.molregno
            where
                mol_dic.pref_name is not null;
        '''

        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        data_dictionary = []

        for row in result:
            row = list(row)
            for index, value in enumerate(row):
                row[index] = str(value) if value is not None else ''
                
            molecular_uid, molecule_name, chembl_id, molecule_type, first_approval, max_phase, full_weight, monoisotopic_weight, parent_compound_weight, alogp, rotatable_bonds, polar_surface_area, molecular_species, hba, hbd, ro5_violations, hba_lipinski, hbd_lipinski, ro5_violations_lipinski, cx_acidic_pka, cx_basic_pka, cx_logp, cx_logd, aromatic_rings, heavy_atoms, qed_weighted, np_likeness_score, full_molecular_formula, standard_inchi, standard_inchi_key, canonical_smiles = row

            data_dictionary.append({
                'molecular_uid': molecular_uid,
                'molecule_name': molecule_name,
                'chembl_id': chembl_id,
                'molecule_type': molecule_type,
                'first_approval': first_approval,
                'max_phase': max_phase,
                'full_weight': full_weight,
                'monoisotopic_weight': monoisotopic_weight,
                'parent_compound_weight': parent_compound_weight,
                'alogp': alogp,
                'rotatable_bonds': rotatable_bonds,
                'polar_surface_area': polar_surface_area,
                'molecular_species': molecular_species,
                'hba': hba,
                'hbd': hbd,
                'ro5_violations': ro5_violations,
                'hba_lipinski': hba_lipinski,
                'hbd_lipinski': hbd_lipinski,
                'ro5_violations_lipinski': ro5_violations_lipinski,
                'cx_acidic_pka': cx_acidic_pka,
                'cx_basic_pka': cx_basic_pka,
                'cx_logp': cx_logp,
                'cx_logd': cx_logd,
                'aromatic_rings': aromatic_rings,
                'heavy_atoms': heavy_atoms,
                'qed_weighted': qed_weighted,
                'np_likeness_score': np_likeness_score,
                'full_molecular_formula': full_molecular_formula,
                'standard_inchi': standard_inchi,
                'standard_inchi_key': standard_inchi_key,
                'canonical_smiles': canonical_smiles
            })

        return data_dictionary

 
    @staticmethod
    def get_compound_synonyms() -> list[dict[str, str]]:
        connection = connect()
        cursor = connection.cursor()
        
        query = '''
            select
                mol_dic.molregno as molecular_uid,
                mol_syn.synonyms as molecule_synonym
            from 
                public.molecule_dictionary as mol_dic full join 
                public.molecule_synonyms as mol_syn on mol_dic.molregno = mol_syn.molregno
            where
                mol_dic.pref_name is not null;
        '''

        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        data_dictionary = []

        for row in result:
            row = list(row)
            for index, value in enumerate(row):
                row[index] = str(value) if value is not None else ''

            molecular_uid, molecule_synonym = row

            data_dictionary.append({
                'molecular_uid': molecular_uid,
                'molecule_synonym': molecule_synonym
            })

        return data_dictionary


    @staticmethod
    def get_drug_mechanism_info() -> list[dict[str, str]]:
        connection = connect()
        cursor = connection.cursor()

        query = '''
            select
                mol_dic.molregno as molecular_uid,
                drg_mec.action_type as action_type,
                drg_mec.mechanism_of_action as mechanism_of_action,
                drg_mec.tid as target_uid,
                mec_ref.ref_type as reference_type,
                mec_ref.ref_id as reference_id,
                mec_ref.ref_url as reference_url,
                com_rec.compound_name as compound_name,
                com_rec.compound_key as compound_key,
                src.src_short_name as source_name,
                src.src_description as source_description
            from 
                public.molecule_dictionary as mol_dic full join 
                public.drug_mechanism as drg_mec on mol_dic.molregno = drg_mec.molregno full join
                public.mechanism_refs as mec_ref on drg_mec.mec_id = mec_ref.mec_id full join
                public.compound_records as com_rec on drg_mec.record_id = com_rec.record_id full join
                public.source as src on com_rec.src_id = src.src_id
            where
                mol_dic.pref_name is not null;
        '''

        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        data_dictionary = []

        for row in result:
            row = list(row)
            for index, value in enumerate(row):
                row[index] = str(value) if value is not None else ''

            molecular_uid, action_type, mechanism_of_action, target_uid, reference_type, reference_id, reference_url, compound_name, compound_key, source_name, source_description = row

            data_dictionary.append({
                'molecular_uid': molecular_uid,
                'action_type': action_type,
                'mechanism_of_action': mechanism_of_action,
                'target_uid': target_uid,
                'reference_type': reference_type,
                'reference_id': reference_id,
                'reference_url': reference_url,
                'compound_name': compound_name,
                'compound_key': compound_key,
                'source_name': source_name,
                'source_description': source_description
            })

        return data_dictionary
    

    @staticmethod
    def get_drug_indication_info() -> list[dict[str, str]]:
        connection = connect()
        cursor = connection.cursor()

        query = '''
            select
                mol_dic.molregno as molecular_uid,
                drg_ind.mesh_id as mesh_id,
                drg_ind.mesh_heading as mesh_heading,
                case
                    when drg_ind.max_phase_for_ind is null then 'Preclinical Compound'
                    when drg_ind.max_phase_for_ind = -1 then 'Clinical Phase Unknown'
                    when drg_ind.max_phase_for_ind = 0.5 then 'Early Phase 1 Clinical Trials'
                    when drg_ind.max_phase_for_ind = 1 then 'Phase 1 Clinical Trials'
                    when drg_ind.max_phase_for_ind = 2 then 'Phase 2 Clinical Trials'
                    when drg_ind.max_phase_for_ind = 3 then 'Phase 3 Clinical Trials'
                    when drg_ind.max_phase_for_ind = 4 then 'Approved'
                end
                as max_phase_for_ind,
                ind_ref.ref_type as reference_type,
                ind_ref.ref_id as reference_id,
                ind_ref.ref_url as reference_url,
                com_rec.compound_name as compound_name,
                com_rec.compound_key as compound_key,
                src.src_short_name as source_name,
                src.src_description as source_description
            from 
                public.molecule_dictionary as mol_dic full join 
                public.drug_indication as drg_ind on mol_dic.molregno = drg_ind.molregno full join
                public.indication_refs as ind_ref on drg_ind.drugind_id = ind_ref.drugind_id full join
                public.compound_records as com_rec on drg_ind.record_id = com_rec.record_id full join
                public.source as src on com_rec.src_id = src.src_id
            where
                mol_dic.pref_name is not null;
        '''

        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        data_dictionary = []

        for row in result:
            row = list(row)
            for index, value in enumerate(row):
                row[index] = str(value) if value is not None else ''

            molecular_uid, mesh_id, mesh_heading, max_phase_for_ind, reference_type, reference_id, reference_url, compound_name, compound_key, source_name, source_description = row

            data_dictionary.append({
                'molecular_uid': molecular_uid,
                'mesh_id': mesh_id,
                'mesh_heading': mesh_heading,
                'max_phase_for_ind': max_phase_for_ind,
                'reference_type': reference_type,
                'reference_id': reference_id,
                'reference_url': reference_url,
                'compound_name': compound_name,
                'compound_key': compound_key,
                'source_name': source_name,
                'source_description': source_description
            })

        return data_dictionary