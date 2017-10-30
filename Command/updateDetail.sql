<<<<<<< HEAD
=======
/* On ne traite plus les requetes maritimes via TratFileCommand car il faudrait ajouter le traitement des tables delivery_header et detail*/

/*
>>>>>>> e5edb8332aef90e908d12ee0a33c02843e688e62
UPDATE tracing_container_detail_mer SET
stripped_sku = CAST(TRIM('[[pcs]]') AS INTEGER), 
stripped_ctn = CAST(TRIM('[[ctn]]') AS INTEGER)
WHERE container = TRIM('[[shptRef]]')
AND LOWER(num_po) LIKE TRIM(LOWER('[[numPoRoot]]'))||'%'
AND sku_id = TRIM('[[sku]]')
;
<<<<<<< HEAD
=======
*/
>>>>>>> e5edb8332aef90e908d12ee0a33c02843e688e62

UPDATE cp_loading_po_sku SET
pcs_received = CAST(TRIM('[[pcs]]') AS INTEGER), 
ctn_received = CAST(TRIM('[[ctn]]') AS INTEGER)
WHERE idloading = (
    SELECT idloading
    FROM cp_loading
    WHERE ref =  TRIM('[[shptRef]]')
)
AND LOWER(num_po) LIKE TRIM(LOWER('[[numPoRoot]]'))||'%'
AND sku_id = TRIM('[[sku]]')
;
