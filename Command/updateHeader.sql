UPDATE tracing_container_po_mer SET
fnd_confirmed_date = '[[whseArrivaleDate]]', 
fnd_confirmed_time = '[[whseArrivaleTime]]'
WHERE container = TRIM('[[shptRef]]')
;

UPDATE cp_loading SET 
delivery_real_date = '[[whseArrivaleDate]]', 
delivery_real_time = '[[whseArrivaleTime]]'
WHERE ref =  TRIM('[[shptRef]]')
;
