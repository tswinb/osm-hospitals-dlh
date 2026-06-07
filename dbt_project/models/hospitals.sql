with
    -- Source
    bronze_hospitals as (select * from {{ source("bronze", "hospitals") }}),
    -- Select and parse
    raw_files as (
        select
            filename as bronze_path,
            country,
            -- extract run timestamp from the S3 path:
            -- s3://bronze/{country_slug}/{YYYY-MM-DDTHH-MM-SS}/hospitals.geojson
            strptime(
                regexp_extract(
                    filename,
                    '/([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}-[0-9]{2}-[0-9]{2})/',
                    1
                ),
                '%Y-%m-%dT%H-%M-%S'
            ) as run_ts,
            airflow_run_id,
            features
        from bronze_hospitals
    ),
    -- Deduplicate on country
    latest_files as (
        -- for each country, keep only the most recent run's file
        select * exclude (rn)
        from
            (
                select
                    *,
                    row_number() over (partition by country order by run_ts desc) as rn
                from raw_files
            )
        where rn = 1
    ),
    -- Unnest Features
    hospital_elements as (
        select airflow_run_id, country, bronze_path, run_ts, feature.value as feature
        from latest_files, unnest(features) as feature(value)
    ),
    -- Naming, Casting and Null-handling
    cleaned_hospital_elements as (
        select
            md5(
                feature['geometry']['coordinates'][1]::varchar
                || ','
                || feature['geometry']['coordinates'][2]::varchar
            ) as id,
            coalesce(feature['properties']['name']::varchar, 'Unknown') as name,
            -- ST_Point takes (x,y) i.e. (longitude, latitude)
            -- 'coordinates' field from API is also (longitude, lattitude)
            st_point(
                feature['geometry']['coordinates'][1]::double,
                feature['geometry']['coordinates'][2]::double
            )::geometry('OGC:CRS84') as geometry,
            st_transform(
                st_point(
                    feature['geometry']['coordinates'][1]::double,
                    feature['geometry']['coordinates'][2]::double
                ),
                'EPSG:4326',
                'EPSG:3035',
                true
            )::geometry('EPSG:3035') as geometry_3035,
            country,
            coalesce(feature['properties']['addr:city']::varchar, 'Unknown') as city,
            coalesce(
                feature['properties']['addr:street']::varchar, 'Unknown'
            ) as street,
            coalesce(
                feature['properties']['addr:postcode']::varchar, 'Unknown'
            ) as postcode,
            coalesce(
                feature['properties']['addr:housenumber']::varchar, 'Unknown'
            ) as house_number,
            coalesce(
                feature['properties']['healthcare:speciality']::varchar, 'Unknown'
            ) as healthcare_specialty,
            coalesce(feature['properties']['website']::varchar, 'Unknown') as website,
            coalesce(feature['properties']['operator']::varchar, 'Unknown') as operator,
            airflow_run_id,
            run_ts::timestamptz as bronze_loaded_at,
            bronze_path,
            current_timestamp::timestamptz as silver_loaded_at
        from hospital_elements
    )

-- Final Select
-- DISTINCT ON deduplicates hospitals that appear in more than one country's query
-- result
select distinct
    on (id)
    id,
    name,
    geometry,
    geometry_3035,
    country,
    city,
    street,
    postcode,
    house_number,
    healthcare_specialty,
    website,
    operator,
    -- audit columns
    airflow_run_id,
    bronze_loaded_at,
    bronze_path,
    silver_loaded_at
from cleaned_hospital_elements
order by bronze_loaded_at desc
