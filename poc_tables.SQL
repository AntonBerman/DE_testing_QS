-- This is where you will write the SQL to create the tables needed by the bar staff to assist on restocking decisions

SELECT b.name as bar_name
     , d.name as coctail_name
     , g.name as glass
     , s.stock as current_stock_glasses
     , gr.count_coctails as last_date_consumption
FROM drinks d
JOIN (
    SELECT bars_id, drink_id, count(*) as count_coctails 
    FROM bars_transactions
    WHERE strftime('%Y-%m-%d', datetime) in (
        SELECT max(strftime('%Y-%m-%d', datetime)) as last_date FROM bars_transactions)
    GROUP BY bars_id, drink_id
    ORDER BY bars_id) gr
ON gr.drink_id = s.id 
JOIN stocks s 
ON s.bars_id = gr.bars_id 
AND s.glass_type_id = d.glass_type_id
JOIN bars b 
ON b.id = gr.bars_id
JOIN glasstype g 
ON g.id = d.glass_type_id;