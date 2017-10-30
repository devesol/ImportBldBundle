/* On ne traite plus les requetes maritimes via TratFileCommand car il faudrait ajouter le traitement des tables delivery_header et detail*/
/*Arrived => R */
/*
UPDATE tracing_container_po_mer SET
statut = '[[statut]]'
fnd_confirmed_date = '[[whseArrivaleDate]]', 
fnd_confirmed_time = '[[whseArrivaleTime]]'
WHERE container = TRIM('[[shptRef]]')
AND ( statut = 'S' OR statut = 'P' OR statut = 'D' )
AND '[[statut]]' = 'R'
;
*/
/*Stripping => F */
/*

UPDATE tracing_container_po_mer SET
statut = '[[statut]]'
fnd_arrival_date = '[[whseArrivaleDate]]', 
fnd_arrival_time = '[[whseArrivaleTime]]'
WHERE container = TRIM('[[shptRef]]')
AND statut = 'R'
AND '[[statut]]' = 'F'
;
*/
UPDATE cp_loading SET 
delivery_real_date = '[[whseArrivaleDate]]', 
delivery_real_time = '[[whseArrivaleTime]]'
WHERE ref =  TRIM('[[shptRef]]')
;
