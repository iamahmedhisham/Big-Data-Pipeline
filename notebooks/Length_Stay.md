## Calculate the length of stay (in hours) for each admission

SELECT

hadm_id,
    
admission_type,
    
(UNIX_TIMESTAMP(dischtime) - UNIX_TIMESTAMP(admittime)) / 3600 AS stay_hours
    
FROM admissions

WHERE dischtime IS NOT NULL

ORDER BY stay_hours DESC

LIMIT 10;
