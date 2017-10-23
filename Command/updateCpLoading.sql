UPDATE cp_loading SET 
delivery_real_date = '[[whseArrivaleDate]]', 
delivery_real_time = '[[whseArrivaleTime]]'
WHERE ref =  TRIM('[[shptRef]]')
;
