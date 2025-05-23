## Calculate average time from callout creation to acknowledgment:

SELECT callout_service , AVG(UNIX_TIMESTAMP(acknowledgetime) - UNIX_TIMESTAMP(createtime)) / 3600 AS avg_acknowledge_hours

FROM callout

WHERE acknowledgetime IS NOT NULL

GROUP BY callout_service

ORDER BY avg_acknowledge_hours DESC;
