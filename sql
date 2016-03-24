
--Copy entire note events

\copy (WITH tmp as (SELECT adm.subject_id AS subject_id, adm.hadm_id AS hadm_id, admittime, dischtime, adm.deathtime,pat.gender AS gender, pat.dob AS dob, pat.dod AS dod, ROW_NUMBER() OVER (PARTITION BY hadm_id ORDER BY admittime DESC) AS mostrecent FROM admissions adm INNER JOIN patients pat ON adm.subject_id = pat.subject_id WHERE lower(diagnosis) NOT LIKE '%organ donor%' AND extract(YEAR FROM admittime) - extract(YEAR FROM dob) > 15 AND HAS_CHARTEVENTS_DATA = 1) SELECT tmp.subject_id AS SUBJECT_ID, tmp.hadm_id AS HADM_ID , chartdate, charttime, storetime, category, description, regexp_replace(noteevents.text, E'[\\n\\r]+', ' ', 'g' ) as docs FROM tmp INNER JOIN noteevents ON tmp.hadm_id = noteevents.hadm_id) TO 'test.csv' DELIMITER ',' FORMAT 'CSV' HEADER






--Copy First Charttime for each HADM

\copy (WITH tmp as (SELECT adm.subject_id AS subject_id, adm.hadm_id AS hadm_id, admittime, dischtime, adm.deathtime,pat.gender AS gender, pat.dob AS dob, pat.dod AS dod, ROW_NUMBER() OVER (PARTITION BY hadm_id ORDER BY admittime DESC) AS mostrecent FROM admissions adm INNER JOIN patients pat ON adm.subject_id = pat.subject_id WHERE lower(diagnosis) NOT LIKE '%organ donor%' AND extract(YEAR FROM admittime) - extract(YEAR FROM dob) > 15 AND HAS_CHARTEVENTS_DATA = 1) SELECT tmp.subject_id AS SUBJECT_ID, tmp.hadm_id AS HADM_ID , chartdate, charttime, storetime, category, description, regexp_replace(noteevents.text, E'[\\n\\r]+', ' ', 'g' ) as docs FROM tmp INNER JOIN (WITH summary AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY p.hadm_id ORDER BY p.charttime ASC) AS rk FROM noteevents p) SELECT s.* FROM summary s WHERE s.rk = 1) as noteevents ON tmp.hadm_id = noteevents.hadm_id LIMIT 1) TO 'test.csv' DELIMITER ',' FORMAT 'CSV' HEADER
