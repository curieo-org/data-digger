-- ====================== tbl_studies_info ================================

SELECT
	studies.nct_id,
	studies.brief_title as title,
	concat(bs.description, dd.description) as description,
	json_build_object(
		'source', studies.source,
		'study_type', studies.study_type,
		'enrollment', studies.enrollment,
		'status', studies.overall_status,
		'phase', studies.phase,
		'posted_date', json_build_object(
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
	tsi.nct_id as id,
	tsi.title as title,
	tsi.description as description,
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
		json_agg(sc.collab_details) as CollaboratorDetails
	FROM
		temp_sponsors_collab sc
	GROUP BY sc.nct_id)
SELECT 
	tsi.nct_id as id,
	tsi.title as title,
	tsi.description as description,
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
		jsonb_object_agg('EventType', COALESCE(ret.event_type, 'NA')) ||
		jsonb_object_agg('Classification', COALESCE(ret.classification, 'NA')) ||
		jsonb_object_agg('SubjestAffected', COALESCE(ret.subjects_affected, -1)) ||
		jsonb_object_agg('SubjectsRisk', COALESCE(ret.subjects_at_risk, -1)) as adverse_details
	FROM 
		ctgov.studies studies
	LEFT JOIN ctgov.result_groups rg ON studies.nct_id = rg.nct_id
	LEFT JOIN ctgov.reported_event_totals ret ON studies.nct_id = ret.nct_id AND rg.ctgov_group_code = ret.ctgov_group_code
	WHERE rg.result_type = 'Reported Event'
	group by studies.nct_id, rg.ctgov_group_code)
SELECT 
	tsi.nct_id as id,
	tsi.title as title,
	tsi.description as description,
	tad.adverse_details as adverse_details
INTO
	public.tbl_studies_adverse_details
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_adverse_details tad ON tsi.nct_id = tad.nct_id;



-- ============= tbl_studies_pubmued_links ===============================
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
	tsi.nct_id as id,
	tsi.title as title,
	tsi.description as description,
	tsd.PubmedCitation as PubmedCitation
INTO
	public.tbl_studies_pubmued_links
FROM 
	public.tbl_studies_info tsi
LEFT JOIN tmp_study_details tsd ON tsi.nct_id = tsd.nct_id;


-- ============= tbl_studies_conditions ===============================
SELECT 
	tsi.nct_id as id,
	tsi.title as title,
	tsi.description as description,
	string_agg(distinct cond.downcase_name, ',') as condition_name
INTO
	public.tbl_studies_conditions
FROM 
	public.tbl_studies_info tsi
LEFT JOIN ctgov.conditions cond ON tsi.nct_id = cond.nct_id
GROUP BY tsi.nct_id, tsi.title, tsi.description;
