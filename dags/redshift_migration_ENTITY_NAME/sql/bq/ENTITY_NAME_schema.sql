CREATE TABLE IF NOT EXISTS redshift_eu.ENTITY_NAME
(

    checksum        STRING OPTIONS (DESCRIPTION ="RedShift MD5 checksum of concatenated columns"),
    insert_time     TIMESTAMP OPTIONS (DESCRIPTION ="RedShift column which is used for fetching new records incrementally"),
    export_datetime TIMESTAMP OPTIONS (DESCRIPTION ="Datetime of the export from RedShift")
)
    PARTITION BY DATE (insert_time);
