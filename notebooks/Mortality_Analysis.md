## Examine mortality rates by admission type or location:

SELECT

admission_type,
 
 COUNT(*) AS total_admissions,
  
SUM(CASE WHEN hospital_expire_flag THEN 1 ELSE 0 END) AS deaths,
  
(SUM(CASE WHEN hospital_expire_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS mortality_rate
FROM admissions

GROUP BY admission_type

ORDER BY mortality_rate DESC;
