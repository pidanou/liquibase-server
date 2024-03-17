from pyliquibase import Pyliquibase
import os
import requests
from flask import Flask
from flask import request
from werkzeug.utils import secure_filename
from google.cloud import storage
import psycopg
from psycopg import sql

app = Flask(__name__)


DB_URL = os.environ["DB_URL"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
SPB_URL = os.environ["SPB_URL"]
SPB_SERVICE_KEY = os.environ["SPB_SERVICE_KEY"]
ENV = os.environ["ENV"]
JDBC_URL = f"jdbc:postgresql://{DB_URL}:5432/postgres"


def generate_liquibase_properties_file(file, changelog_file_name):
    f = open(file, "w")
    f.write(f"changelog-file={changelog_file_name}" + "\n")
    f.write(f"url={JDBC_URL}" + "\n")
    f.write(f"username={DB_USER}" + "\n")
    f.write(f"password={DB_PASSWORD}" + "\n")
    f.write("driver=org.postgresql.Driver")
    return file


def download_changelog_from_bucket(bucket, source_file_name, destination_file_name):
    try:
        storage_client = storage.Client(f"shelfctrl-{ENV}")
        bucket = storage_client.bucket(bucket)
        file = bucket.blob(source_file_name)
        file.download_to_filename(destination_file_name)
    except Exception as err:
        print(str(err))


def get_all_tenant_schemas():
    header = {
        "Accept-Profile": "public",
        "apikey": SPB_SERVICE_KEY,
        "Authorization": f"Bearer {SPB_SERVICE_KEY}"
    }
    try:
        response = requests.get(
            url=SPB_URL+"/rest/v1/Store?select=id", headers=header).json()

    except Exception as err:
        print(str(err))
        raise err
    return response


schema_update_message = {"message": "Error while updating schema"}

def allowed(req):
    if req.headers.get("Authorization") == f"Bearer {SPB_SERVICE_KEY}":
        return True 
    return False


@app.route("/update_schema", methods=['PATCH'])
def update_schema():

    if not allowed(request):
        return {"message": "Not allowed"}, 403
    
    data = request.get_json()

    try:
        store_id = data["store_id"]
        table_changelog_file = data["table_changelog_file"]
        rls_changelog_file = data["rls_changelog_file"]
    except Exception:
        return {"message": "Incomplete request"}, 500

    try:
        download_changelog_from_bucket(
            f"shelfctrl-{ENV}", table_changelog_file, secure_filename(table_changelog_file))
        download_changelog_from_bucket(
            f"shelfctrl-{ENV}", rls_changelog_file, secure_filename(rls_changelog_file))
    except Exception:
        return {"message": "Error while getting changelog"}, 500

    table_liquibase_properties = generate_liquibase_properties_file(
        "table.liquibase.properties", secure_filename(table_changelog_file))
    rls_liquibase_properties = generate_liquibase_properties_file(
        "rls.liquibase.properties", secure_filename(rls_changelog_file))

    table_liquibase = Pyliquibase(
        defaultsFile=table_liquibase_properties, logLevel="INFO")
    rls_liquibase = Pyliquibase(
        defaultsFile=rls_liquibase_properties, logLevel="INFO")

    if store_id != "all-tenant":
        schema = "store_" + store_id.replace('-', '')
        try:
            table_liquibase.execute(
                "update", f"-Dschema_name={schema}",  f"-Dstore_id={store_id}", f"--liquibase-schema-name={schema}")
            rls_liquibase.execute(
                "update", f"-Dschema_name={schema}",  f"-Dstore_id={store_id}", f"--liquibase-schema-name={schema}")
        except Exception as err:
            print(str(err))
            return schema_update_message, 500
        return {"message": "Updated"}, 200

    try:
        all_tenant_schema_id = get_all_tenant_schemas()
    except requests.exceptions.HTTPError as err:
        return err.response.json(), err.response.status_code
    except Exception:
        return {"message": "Unknown error while getting all schema IDs"}, 500

    for tenant_schema_id in all_tenant_schema_id:
        schema = f"store_{tenant_schema_id['id'].replace('-', '')}"
        try:
            table_liquibase.execute(
                "update", f"-Dschema_name={schema}", f"-Dstore_id={tenant_schema_id['id']}", f"--liquibase-schema-name={schema}")
            rls_liquibase.execute(
                "update", f"-Dschema_name={schema}", f"-Dstore_id={tenant_schema_id['id']}", f"--liquibase-schema-name={schema}")
        except Exception:
            return schema_update_message, 500

    return {"message": "Updated all tenants schema"}, 200


@app.route("/create_schema", methods=['POST'])
def create_schema():

    if not allowed(request):
        return {"message": "Not allowed"}, 403
    
    data = request.get_json()

    try:
        store_id = data["store_id"]
        table_changelog_file = data["table_changelog_file"]
        rls_changelog_file = data["rls_changelog_file"]
    except Exception:
        return {"message": "Incomplete request"}, 500

    schema = "store_" + store_id.replace('-', '')

    try:
        download_changelog_from_bucket(
            f"shelfctrl-{ENV}", table_changelog_file, secure_filename(table_changelog_file))
        download_changelog_from_bucket(
            f"shelfctrl-{ENV}", rls_changelog_file, secure_filename(rls_changelog_file))
    except Exception:
        return {"message": "Error while getting changelog"}, 500

    table_liquibase_properties = generate_liquibase_properties_file(
        "table.liquibase.properties", secure_filename(table_changelog_file))
    rls_liquibase_properties = generate_liquibase_properties_file(
        "rls.liquibase.properties", secure_filename(rls_changelog_file))

    table_liquibase = Pyliquibase(
        defaultsFile=table_liquibase_properties, logLevel="INFO")
    rls_liquibase = Pyliquibase(
        defaultsFile=rls_liquibase_properties, logLevel="INFO")

    try:
        psyco = psycopg.connect(
            conninfo=f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_URL}:5432/postgres", autocommit=True)
        psyco.execute(sql.SQL("CREATE SCHEMA {};").format(
            sql.Identifier(schema)))
    except Exception as err:
        print(str(err))
        return {"message": "Error creating schema"}, 500

    try:
        table_liquibase.execute(
            "update", f"-Dschema_name={schema}",  f"-Dstore_id={store_id}", f"--liquibase-schema-name={schema}")
        rls_liquibase.execute(
            "update", f"-Dschema_name={schema}",  f"-Dstore_id={store_id}", f"--liquibase-schema-name={schema}")

    except Exception as err:
        print(str(err))
        return schema_update_message, 500
    return {"message": "Created"}, 200
