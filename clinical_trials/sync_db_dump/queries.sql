-- ====================== tbl_studies_info ================================

SELECT
	studies.nct_id,
	studies.brief_title as title,
	concat(bs.description, dd.description) as description,
	jsonb_build_object(
		'source', studies.source,
		'study_type', studies.study_type,
		'enrollment', studies.enrollment,
		'status', studies.overall_status,
		'phase', studies.phase,
		'posted_date', jsonb_build_object(
			'first_date', studies.study_first_posted_date,
			'last_updated_date', studies.last_update_posted_date
		)
	) as study_details
INTO
	public.tbl_studies_info
FROM 
	ctgov.studies studies
LEFT JOIN ctgov.detailed_descriptions dd ON studies.nct_id = dd.nct_id
LEFT JOIN ctgov.brief_summaries bs ON studies.nct_id = bs.nct_id;


-- ================== tbl_studies_eligibilities ===========================

with temp_studies_eligibilities as (
	SELECT 
	studies.nct_id as nct_id,
	jsonb_object_agg('Population', COALESCE(eli.population, 'NA')) ||
	jsonb_object_agg('SamplingMethod', COALESCE(eli.sampling_method, 'NA')) ||
	jsonb_object_agg('MinimumAge', eli.minimum_age) ||
	jsonb_object_agg('MaximumAge', eli.maximum_age) ||
	jsonb_object_agg('HealthyVolunteers', eli.healthy_volunteers) ||
	jsonb_object_agg('Criteria', REPLACE(eli.criteria, E'\n', ' ')) as eligibility
	FROM 
		ctgov.studies studies
	LEFT JOIN ctgov.eligibilities eli ON studies.nct_id = eli.nct_id	 
	group by studies.nct_id
)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	tse.eligibility as eligibility_details
INTO
	public.tbl_studies_eligibilities
FROM 
	public.tbl_studies_info tsi
LEFT JOIN temp_studies_eligibilities tse ON tsi.nct_id = tse.nct_id;


-- ================== tbl_studies_sponsors ===========================

with studies_sponsors_collab as(
	with temp_sponsors_collab as (
		SELECT 
			studies.nct_id as nct_id,
			jsonb_object_agg('CollaboratorType', spon.lead_or_collaborator) ||
			jsonb_object_agg('CollaboratorDetails', spon.name) as collab_details
				FROM 
					ctgov.studies studies
				LEFT JOIN ctgov.sponsors spon ON studies.nct_id = spon.nct_id
				GROUP BY studies.nct_id, spon.lead_or_collaborator
	)
	SELECT 
		sc.nct_id as nct_id,
		jsonb_agg(sc.collab_details) as CollaboratorDetails
	FROM
		temp_sponsors_collab sc
	GROUP BY sc.nct_id)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	ssc.CollaboratorDetails as CollaboratorDetails
INTO
	public.tbl_studies_sponsors
FROM 
	public.tbl_studies_info tsi
LEFT JOIN studies_sponsors_collab ssc ON tsi.nct_id = ssc.nct_id;


-- ============= tbl_studies_adverse_details ===============================

with tmp_adverse_details as (
	SELECT 
		studies.nct_id,
		rg.ctgov_group_code as ctgov_group_code,
		jsonb_object_agg('EventType', COALESCE(ret.event_type, 'NA')) ||
		jsonb_object_agg('Classification', COALESCE(ret.classification, 'NA')) ||
		jsonb_object_agg('SubjectAffected', COALESCE(ret.subjects_affected, -1)) ||
		jsonb_object_agg('SubjectsRisk', COALESCE(ret.subjects_at_risk, -1)) as adverse_details
	FROM 
		ctgov.studies studies
	LEFT JOIN ctgov.result_groups rg ON studies.nct_id = rg.nct_id
	LEFT JOIN ctgov.reported_event_totals ret ON studies.nct_id = ret.nct_id AND rg.ctgov_group_code = ret.ctgov_group_code
	WHERE rg.result_type = 'Reported Event'
	group by studies.nct_id, rg.ctgov_group_code)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	tad.ctgov_group_code as ctgov_group_code,
	tad.adverse_details as adverse_details
INTO
	public.tbl_studies_adverse_details
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_adverse_details tad ON tsi.nct_id = tad.nct_id;


-- ============= tbl_studies_pubmed_links ===============================

with tmp_study_details as (
	SELECT 
		studies.nct_id as nct_id,
		jsonb_object_agg('Pubmed', COALESCE(sr.pmid, 'NA')) ||
		jsonb_object_agg('Citation', sr.citation) as PubmedCitation
	FROM 
		ctgov.studies studies
	LEFT JOIN ctgov.study_references sr ON studies.nct_id = sr.nct_id		
	group by studies.nct_id)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	tsd.PubmedCitation as PubmedCitation
INTO
	public.tbl_studies_pubmed_links
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_study_details tsd ON tsi.nct_id = tsd.nct_id;


-- ============= tbl_studies_conditions ===============================

SELECT 
	tsi.nct_id as nct_id,
	string_agg(tsi.title, ',') as title,
	string_agg(distinct cond.downcase_name, ',') as condition_name
INTO
	public.tbl_studies_conditions
FROM 
	public.tbl_studies_info tsi
LEFT JOIN ctgov.conditions cond ON tsi.nct_id = cond.nct_id
GROUP BY tsi.nct_id;


-- ============= tbl_primary_outcome_measurement =====================

with tmp_tbl_primary_measurements as (
	SELECT 
		studies.nct_id,
		jsonb_object_agg('Title', COALESCE(oc.title, 'NA')) ||
		jsonb_object_agg('Description', COALESCE(oc.description, 'NA')) ||
		jsonb_object_agg('Time', COALESCE(oc.time_frame, 'NA')) ||
		jsonb_object_agg('Population', COALESCE(oc.population, 'NA')) ||
		jsonb_object_agg('Units', COALESCE(oc.units, 'NA')) as outcome_primary_measurement_details,
		jsonb_object_agg('Value', COALESCE(om.param_value, 'NA')) ||
		jsonb_object_agg('Type', COALESCE(om.param_type, 'NA')) ||
		jsonb_object_agg('DispersionValue', COALESCE(om.dispersion_value, 'NA')) ||
		jsonb_object_agg('DispersionType', COALESCE(om.dispersion_type, 'NA')) as outcome_primary_measurement_value_details
	FROM 
		ctgov.studies studies
	LEFT JOIN ctgov.outcomes oc ON studies.nct_id = oc.nct_id
	LEFT JOIN ctgov.outcome_measurements om ON studies.nct_id = om.nct_id AND oc.id = om.outcome_id
	WHERE oc.outcome_type = 'Primary'
	group by studies.nct_id)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	ttpm.outcome_primary_measurement_details as Outcome_Primary_Measurement_Details,
	ttpm.outcome_primary_measurement_value_details as Outcome_Primary_Measurement_Value_Details
INTO
	public.tbl_primary_outcome_measurement	
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_tbl_primary_measurements ttpm ON tsi.nct_id = ttpm.nct_id;


-- ============ tbl_secondary_outcome_measurement ==================

with tmp_tbl_secondary_measurements as (
	SELECT 
		studies.nct_id,
		jsonb_object_agg('Title', COALESCE(oc.title, 'NA')) ||
		jsonb_object_agg('Description', COALESCE(oc.description, 'NA')) ||
		jsonb_object_agg('Time', COALESCE(oc.time_frame, 'NA')) ||
		jsonb_object_agg('Population', COALESCE(oc.population, 'NA')) ||
		jsonb_object_agg('Units', COALESCE(oc.units, 'NA')) as outcome_secondary_measurement_details,
		jsonb_object_agg('Value', COALESCE(om.param_value, 'NA')) ||
		jsonb_object_agg('Type', COALESCE(om.param_type, 'NA')) ||
		jsonb_object_agg('DispersionValue', COALESCE(om.dispersion_value, 'NA')) ||
		jsonb_object_agg('DispersionType', COALESCE(om.dispersion_type, 'NA')) as outcome_secondary_measurement_value_details
	FROM 
		ctgov.studies studies
	LEFT JOIN ctgov.outcomes oc ON studies.nct_id = oc.nct_id
	LEFT JOIN ctgov.outcome_measurements om ON studies.nct_id = om.nct_id AND oc.id = om.outcome_id
	WHERE oc.outcome_type = 'Secondary'
	group by studies.nct_id)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	ttsm.outcome_secondary_measurement_details as Outcome_Secondary_Measurement_Details,
	ttsm.outcome_secondary_measurement_value_details as Outcome_Secondary_Measurement_Value_Details
INTO
	public.tbl_secondary_outcome_measurement	
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_tbl_secondary_measurements ttsm ON tsi.nct_id = ttsm.nct_id;


-- ============ tbl_baseline_details ==========================

with tmp_tbl_baseline_details as (
	SELECT 
		studies.nct_id,
		rg.ctgov_group_code as ctgov_group_code,
		bm.title as title,
		jsonb_object_agg('Title', COALESCE(bm.title, 'NA')) ||
		jsonb_object_agg('Classification', COALESCE(bm.classification, 'NA')) ||
		jsonb_object_agg('Category', COALESCE(bm.category, 'NA')) ||
		jsonb_object_agg('Value', COALESCE(bm.param_value, 'NA')) ||
		jsonb_object_agg('Type', COALESCE(bm.param_type, 'NA')) ||
		jsonb_object_agg('DispersionValue', COALESCE(bm.dispersion_value, 'NA')) ||
		jsonb_object_agg('DispersionType', COALESCE(bm.dispersion_type, 'NA')) as baseline_measurement_details,
		jsonb_object_agg('ResultGroupTitle', COALESCE(rg.title, 'NA')) ||
		jsonb_object_agg('ResultGroupDesc', COALESCE(rg.description, 'NA')) ||
		jsonb_object_agg('BaseLineCount', COALESCE(bc.count, 0)) ||
		jsonb_object_agg('BaseLineCountUnits', COALESCE(bc.units, 'NA')) as baseline_group_details
	FROM 
		ctgov.studies studies
	LEFT JOIN ctgov.result_groups rg ON studies.nct_id = rg.nct_id
	LEFT JOIN ctgov.baseline_counts bc ON studies.nct_id = bc.nct_id AND rg.id = bc.result_group_id
	LEFT JOIN ctgov.baseline_measurements bm ON studies.nct_id = bm.nct_id AND rg.id = bm.result_group_id
	WHERE rg.result_type = 'Baseline'
	group by studies.nct_id, rg.ctgov_group_code, bm.title)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	ttbd.ctgov_group_code as ctgov_group_code,
	ttbd.title as baseline_title,
	ttbd.baseline_measurement_details as Baseline_Measurement_Details,
	ttbd.baseline_group_details as Baseline_Group_Details
INTO
	public.tbl_baseline_details	
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_tbl_baseline_details ttbd ON tsi.nct_id = ttbd.nct_id;


-- ============ tbl_studies_interventions ============================

with tmp_tbl_studies_interventions as (
	with studies_interventions as(
		SELECT 
			studies.nct_id as nct_id,
			iv.intervention_type as iv_type,
			jsonb_object_agg('Name', iv.name) ||
			jsonb_object_agg('Description', iv.description) as iv_details
		FROM 
			ctgov.studies studies
		LEFT JOIN ctgov.interventions iv ON studies.nct_id = iv.nct_id
		GROUP BY studies.nct_id, iv.intervention_type)
	SELECT 
		si.nct_id as nct_id,
		jsonb_object_agg('Type', si.iv_type) ||
		jsonb_object_agg('Details', si.iv_details) as study_intervention_compressed_details
	FROM
		studies_interventions si
	GROUP BY si.nct_id)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	ttsi.study_intervention_compressed_details as Study_Intervention_Compressed_Details
INTO
	public.tbl_studies_interventions	
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_tbl_studies_interventions ttsi ON tsi.nct_id = ttsi.nct_id;


-- ============ tbl_studies_arms_details =====================

with tmp_tbl_studies_arms_details as (
	SELECT 
		studies.nct_id as nct_id,
		jsonb_object_agg('GroupType', dg.group_type) ||
		jsonb_object_agg('Title', dg.title) ||
		jsonb_object_agg('Description', dg.description) as arm_details
	FROM 
		ctgov.studies studies
	LEFT JOIN ctgov.design_groups dg ON studies.nct_id = dg.nct_id
	GROUP BY studies.nct_id)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	ttsad.arm_details as Arm_Details
INTO
	public.tbl_studies_arms_details	
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_tbl_studies_arms_details ttsad ON tsi.nct_id = ttsad.nct_id;


-- ============ tbl_studies_design_outcomes ====================

with tmp_tbl_studies_design_outcomes as (
	SELECT 
		studies.nct_id as nct_id,
		jsonb_object_agg('OutcomeType', dou.outcome_type) ||
		jsonb_object_agg('Measure', dou.measure) ||
		jsonb_object_agg('Time', dou.time_frame) ||
		jsonb_object_agg('Description', dou.description) as design_outcome_measures
	FROM 
		ctgov.studies studies
	LEFT JOIN ctgov.design_outcomes dou ON studies.nct_id = dou.nct_id
	GROUP BY studies.nct_id)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	ttsdo.design_outcome_measures as Design_Outcome_Measures	
INTO
	public.tbl_studies_design_outcomes
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_tbl_studies_design_outcomes ttsdo ON tsi.nct_id = ttsdo.nct_id;


-- ============= tbl_studies_designs =================================

with tmp_tbl_studies_designs as (
	SELECT 
		studies.nct_id as nct_id,
		jsonb_object_agg('Allocation', des.allocation) ||
		jsonb_object_agg('Intervention_Model', des.intervention_model) ||
		jsonb_object_agg('Masking', des.masking) ||
		jsonb_object_agg('Primary_Purpose', des.primary_purpose) as design_details
	FROM 
		ctgov.studies studies
	LEFT JOIN ctgov.designs des ON studies.nct_id = des.nct_id
	GROUP BY studies.nct_id)
SELECT 
	tsi.nct_id as nct_id,
	tsi.title as title,
	ttsd.design_details as Design_Details	
INTO
	public.tbl_studies_designs
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_tbl_studies_designs ttsd ON tsi.nct_id = ttsd.nct_id;

-- ================================================================