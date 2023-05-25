SELECT TO_HEX(MD5(COALESCE(CAST(starttime AS STRING), '') || ', ' || COALESCE(CAST(eventname AS STRING), '') || ', ' ||
                  COALESCE(CAST(eventid AS STRING), '') || ', ' || COALESCE(CAST(catid AS STRING), '') || ', ' ||
                  COALESCE(CAST(venueid AS STRING), '') || ', ' || COALESCE(CAST(dateid AS STRING), ''))) =
       checksum as equal_checksum
FROM `<YOUR_GCP_PROJECT_ID>.redshift_raw.event`
WHERE export_datetime = '%(export_datetime)s'