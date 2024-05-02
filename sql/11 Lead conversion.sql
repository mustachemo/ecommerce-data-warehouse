-- LEAD CONVERSION

-- view_table('leads_qualified', 5)
SELECT *
FROM leads_qualified
LIMIT 5;


-- view_table('leads_closed', 5)
SELECT *
FROM leads_closed
LIMIT 5;


-- lead_conversion
SELECT 
    COALESCE(origin, 'unknown') AS origin,
    COUNT(DISTINCT leads_qualified.mql_id) AS qualified_leads,
    COUNT(DISTINCT leads_closed.mql_id) AS closed_leads,
    COUNT(DISTINCT leads_closed.mql_id) * 100.0 / COUNT(DISTINCT leads_qualified.mql_id)
        AS conversion_rate
FROM leads_qualified
    LEFT JOIN leads_closed USING (mql_id)
GROUP BY COALESCE(origin, 'unknown')
ORDER BY COUNT(leads_qualified.mql_id) DESC;
