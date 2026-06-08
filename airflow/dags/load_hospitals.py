# DAG to extract and load a snapshot of hospital locations from OSM API (Overpass)

import json
import logging
import os
import time
from datetime import datetime, timezone
from io import BytesIO

import requests
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from minio import Minio
from minio.error import S3Error
from pendulum import duration

# Each entry is queried as a separate Overpass request to avoid rate-limiting.
# The country field is kept all the way into the silver layer.
# Wikidata IDs can be found at https://www.wikidata.org/wiki/Special:Search
WIKIDATA_AREAS = [
    {"name": "England", "wikidata": "Q21"},
    {"name": "Wales", "wikidata": "Q25"},
    {"name": "Scotland", "wikidata": "Q22"},
    {"name": "Northern Ireland", "wikidata": "Q26"},
]
BUCKET_NAME_BRONZE = "bronze"
BUCKET_NAME_SILVER = "silver"

DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/dbt_project")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/dbt_project")

# Sleep needed to avoid rate limiting
SLEEP_BETWEEN_REQUESTS = 65


def overpass_to_geojson(data: dict) -> dict:
    """
    Convert Overpass API JSON response to a GeoJSON FeatureCollection.
    OSM tags become GeoJSON properties; id and type are preserved with _ prefix.
    """

    features = []
    for element in data.get("elements", []):
        if "geometry" not in element:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": element["geometry"],
                "properties": {
                    **element.get("tags", {}),
                    "_osm_id": element.get("id"),
                    "_osm_type": element.get("type"),
                },
            }
        )
    return {
        "type": "FeatureCollection",
        "features": features,
        "airflow_run_id": data.get("airflow_run_id"),
        "country": data.get("country", "Unknown"),
    }


@dag(
    dag_id="load_hospitals",
    description="Extract hospital locations from OSM and process until silver layer",
    schedule="@monthly",
    start_date=datetime(2026, 6, 6),
    catchup=False,
    max_active_runs=1,
    tags=["hospitals", "osm", "minio", "geospatial"],
)
def load_hospitals():
    """
    DAG to extract a snapshot of hospital locations from OpenStreetMap (OSM)
    using the Overpass API, save raw GeoJSON to MinIO (bronze), and then
    transform it to GeoParquet in MinIO (silver) using dbt.
    """
    logger = logging.getLogger(__name__)

    @task(
        map_index_template="{{ task.op_kwargs['area']['name'] }}",
        max_active_tis_per_dag=2,
        retries=1,
        retry_delay=duration(seconds=65),
    )
    def extract_hospitals(area: str, **context):
        """
        Extracts hospitals from OSM using Overpass API and saves raw geoJSON
        to MinIO (bronze). Areas in WIKIDATA_AREAS are queried separately
        and saved as its own file, partitioned by country slug and timestamp.
        Each task gets data for one area.
        """

        overpass_user_agent = os.getenv("OVERPASS_USER_AGENT")
        overpass_url = "http://overpass-api.de/api/interpreter"

        logger.info("Creating MinIO client...")
        minio_client = Minio(
            endpoint=os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            secure=False,
        )

        # Create buckets. This is a workaround because initialising the buckets
        # in the docker-compose was not working.
        for bucket in [BUCKET_NAME_BRONZE, BUCKET_NAME_SILVER]:
            try:
                minio_client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
            except S3Error as e:
                # Silently ignore race window issue with two simultaneous tasks
                if e.code != "BucketAlreadyOwnedByYou":
                    # fail the whole DAG run on unexpected MinIO error
                    raise AirflowFailException(str(e))

        # for area in WIKIDATA_AREAS:
        logging.info(f"Querying Overpass API for hospitals in {area['name']}...")
        code = area["wikidata"]
        overpass_query = f"""
                        [out:json][timeout:300];
                        area[wikidata="{code}"]->.searchArea;
                        nwr["amenity"="hospital"](area.searchArea);
                        convert item ::=::,::geom=geom(),_osm_type=type();
                        out center tags;
                        """
        try:
            response = requests.get(
                overpass_url,
                params={"data": overpass_query},
                headers={"User-Agent": overpass_user_agent},
                timeout=320,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            raise AirflowException(f"Failed to query Overpass API for {area['name']}: {str(e)}")
        try:
            data = response.json()
            data_count = len(data["elements"])
            logging.info(f"Retrieved {data_count} hospital locations for {area['name']}")
            if data_count == 0:
                raise AirflowException(f"No data returned for {area['name']}")
        except json.JSONDecodeError:
            raise AirflowException(
                f"Failed to decode JSON response for {area['name']}: {response.text}"
            )

        # Partition by country slug and timestamp so every execution produces
        # a new immutable file
        run_ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        country_slug = area["name"].replace(" ", "_").lower()
        object_name = f"{country_slug}/{run_ts}/hospitals.geojson"

        # Convert Overpass JSON to GeoJSON FeatureCollection for bronze zone
        # country is a top-level field so downstream SQL can read it directly
        # without parsing the file path
        geojson = overpass_to_geojson(
            {
                "elements": data["elements"],
                "airflow_run_id": context["run_id"],
                "country": area["name"],
            }
        )

        try:
            logger.info(f"Saving raw geoJSON for {area['name']} to MinIO (bronze)...")
            geojson_str = json.dumps(geojson)
            geojson_encoded = geojson_str.encode("utf-8")
            minio_client.put_object(
                bucket_name=BUCKET_NAME_BRONZE,
                object_name=object_name,
                data=BytesIO(geojson_encoded),
                length=len(geojson_encoded),
            )
        except Exception as e:
            raise AirflowException(f"Failed to save geoJSON to MinIO: {str(e)}")

        # Sleep to avoid hitting Overpass rate limits
        logger.info(f"Sleeping {SLEEP_BETWEEN_REQUESTS} seconds to avoid rate limitting.")
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # Bash Operator to run dbt transformations and tests,
    # processing the raw geoJSON from MinIO (bronze) and writing transformed
    # GeoParquet back to MinIO (silver)
    transform_hospitals = BashOperator(
        task_id="transform_hospitals",
        bash_command=(
            "cd /opt/airflow && dbt build "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # Specify DAG dependencies
    extract_hospitals.expand(area=WIKIDATA_AREAS) >> transform_hospitals


load_hospitals()
