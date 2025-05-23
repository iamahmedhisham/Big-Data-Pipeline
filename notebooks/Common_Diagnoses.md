## Identify the most frequent diagnoses

    SELECT

    diagnosis,
    
    COUNT(*) AS diagnosis_count
    
    FROM admissions
    
    GROUP BY diagnosis
    
    ORDER BY diagnosis_count DESC
    
    LIMIT 5;
