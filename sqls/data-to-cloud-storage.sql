DECLARE date_run DATE DEFAULT DATE('2014-02-28');
DECLARE table_name STRING;
DECLARE uri STRING;
DECLARE uri_template STRING;

SET table_name = 'tlc_green_trips_2014';
SET uri_template = 'gs://new_york_taxi_trips/%s/%t/*.csv';

WHILE date_run < DATE('2014-03-08') DO
  SET date_run = DATE_ADD(date_run, INTERVAL 1 DAY);

  SET uri = FORMAT(uri_template,
                  table_name,
                  FORMAT_DATE('%Y/%m/%d', date_run));

  EXECUTE IMMEDIATE FORMAT(
    """
    EXPORT DATA OPTIONS(
      uri='%s',
      format='CSV',
      compression='NONE',
      overwrite=true,
      header=true
    ) AS
    SELECT *
    FROM bigquery-public-data.new_york_taxi_trips.%s
    WHERE 1=1
    AND DATE(pickup_datetime) >= '%s'
    AND DATE(pickup_datetime) < DATE_ADD('%s', INTERVAL 1 DAY)
    """,
    uri,
    table_name,
    CAST(date_run AS STRING),
    CAST(date_run AS STRING)
  );
END WHILE;
