-- Analyze ED stay duration for admissions with ED data:

SELECT

    DATE_FORMAT(admittime, 'yyyy-MM-dd') AS admission_date,
    
    COUNT(*) AS admission_count
    
    FROM admissions
    
    GROUP BY DATE_FORMAT(admittime, 'yyyy-MM-dd')
    
    ORDER BY admission_date;
