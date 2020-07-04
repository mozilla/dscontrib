from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import logging


# Bucket Stuff

def upload_to_gcs_bucket(file, bucket, blob_name, project):
    # TODO: Add params descriptions and documentation
    logging.info(f'Starting upload of {file} to {bucket}')
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(filename=file)
    gcs_location = f'gs://{bucket}/{blob_name}'
    logging.info(f'File successfully uploaded to {gcs_location}')
    return gcs_location


def list_files_in_bucket(bucket_name, prefix):
    # Docs: https://cloud.google.com/storage/docs/listing-objects
    """
    Function used to extract a list of file names stored in a google cloud bucket
    :param bucket_name: Name of bucket
    :param prefix: Specify level of files to retrieve. To see all prefix=''.
                To see files in a specific folder prefix='folder1/folder2'
                <- NOTE no '/' at beginning
    :return: list object with file names
    """

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    file_list = []

    for blob in blobs:
        gcs_file = blob.name
        file_list.append(gcs_file)
    return file_list


# Dataset Operations

def create_bq_dataset(project_name, dataset_name, job_name):
    """
    Used to create a new bq dataset
    :param project_name: Name of project that the dataset will be created in
    :param dataset_name: Name of dataset to be created
    :param job_name: Name of job creating the dataset
    :return: None
    """
    logging.info(f'{job_name}: Starting job to create dataset {dataset_name}')
    client = bigquery.Client(project=project_name)
    dataset_ref = client.dataset(dataset_name)

    try:
        logging.info(f'{job_name}: Checking to see if {dataset_name} dataset exists')
        client.get_dataset(dataset_ref)
        logging.info(f'{job_name}: Dataset {dataset_name} exists - no need to create')
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)
        logging.info(f'{job_name}: Successfully created dataset {dataset_name}')
    return None


def update_dataset_documentation():
    return None


# Table Operations
def create_bq_table(project_name, dataset_name, table_name, table_schema, job_name):
    """
    Used to create a bq table
    :param project_name:
    :param dataset_name:
    :param table_name:
    :param table_schema:
    :param job_name:
    :return:
    """
    # TODO: Add params descriptions and documentation
    # TODO: add a process to partition table as well as cluster for efficiency
    logging.info(f'{job_name}: Starting to create table {table_name}')
    client = bigquery.Client(project=project_name)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    try:
        logging.info(f'{job_name}: Checking to see if {table_name} table exists')
        client.get_table(table_ref)
        logging.info(f'{job_name}: Table {table_name} exists - no need to create')
    except NotFound:
        table = bigquery.Table(table_ref, schema=table_schema)
        client.create_table(table)
        logging.info(f'{job_name}: Table {table_name} successfully '
                     f'created in {project_name}.{dataset_name}')
    return None


def load_csv_data_from_gcs_bucket(project_name, dataset_id, table_name, schema,
                                  write_disposition, gcs_file_name, job_name):
    """
    Args:
        project_name:
        dataset_id:
        table_name:
        schema:
        write_disposition:
        gcs_file_name:
        job_name:

    Returns:
    """
    logging.info(f'{job_name}: Starting job to load {gcs_file_name} '
                 f'from google cloud storage to Bigquery')
    client = bigquery.Client(project=project_name)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    load_job_config = bigquery.LoadJobConfig()
    load_job_config.source_format = bigquery.SourceFormat.CSV
    load_job_config.schema = schema
    load_job_config.field_delimiter = ','
    # TODO: How to trigger these as options in the function using **args and **kwargs
    # load_job_config.quote_character = '"'
    # load_job_config.allow_quoted_newlines = True
    # load_job_config.skip_leading_rows = 1
    # load_job_config.max_bad_records = 20
    load_job_config.write_disposition = write_disposition

    job = client.load_table_from_uri(
        gcs_file_name,
        table_ref,
        location='US',
        job_config=load_job_config
    )

    job.result()
    logging.info(f'{job_name}: Loaded {job.output_rows} rows from '
                 f'file {gcs_file_name} to {table_ref}')
    return None


def load_dataframe_to_bq_table(dataframe, project_name, dataset_id, table_name,
                               write_disposition, job_name):
    """
    Args:
        dataframe:
        project_name:
        dataset_id:
        table_name:
        write_disposition:
        job_name:
    Returns:
    """
    logging.info(f'{job_name}: Starting load of dataframe '
                 f'{dataframe} into table {table_name}')
    client = bigquery.Client(project=project_name)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = write_disposition

    job = client.load_table_from_dataframe(
        dataframe,
        table_ref,
        location='US',
        job_config=job_config
    )
    job.result()
    logging.info(f'{job_name}: Dataframe {dataframe} loaded '
                 f'{job.output_rows} into {table_ref.path}')
    return None


def get_list_of_bq_tables(job_name, project_name, dataset_id):
    """
    Function retrieves list of tables from specified project and dataset
    Args:
        job_name: Used for logging purposes.
        Can be name of script or manual.
        project_name: Bigquery project ID
        dataset_id: Bigquery dataset name
    Returns: Pandas dataframe with columns project_id, dataset_name,
    table_name and list of tables in the dataset
    """
    logging.info(f'{job_name}: Starting check for tables in project {project_name} '
                 f'for dataset = {dataset_id}')
    client = bigquery.Client(project=project_name)

    # Create dataframe to append table data to
    columns = ['project_id', 'dataset_name', 'table_name']
    table_list_df = pd.DataFrame(columns=columns)

    # Check if dataset exits
    try:
        logging.info(f'{job_name}: Checking to see if '
                     f'dataset {dataset_id} exists')
        client.get_dataset(dataset_id)
        table_list = client.list_tables(dataset_id)

        for table in table_list:
            table_dict = {'project_id': table.project,
                          'dataset_name': table.dataset_id,
                          'table_name': table.table_id}

            # Create dataframe for each table
            table_df = pd.DataFrame([table_dict])
            table_df.append(table_df)

            table_list_df = table_list_df.append(table_df, ignore_index=True)
    except NotFound:
        logging.info(f'{job_name}: Dataset {dataset_id} '
                     f'Not Found - please check naming')
    return table_list_df


# Query Operations

def calc_last_load_date(project_name, dataset_id, table_name,
                        date_field_name, job_name):
    logging.info(f'{job_name}: Starting check for last data loaded into {table_name}')

    client = bigquery.Client(project=project_name)
    job_config = bigquery.QueryJobConfig()

    # TODO: How to deal with when you need to insert a where clause to filter
    sql = f"""
    SELECT
    max({date_field_name}) AS last_load_date
    FROM
    `{project_name}.{dataset_id}.{table_name}`
    """
    # Run the query
    read_query = client.query(
        sql,
        location='US',
        job_config=job_config)  # API request - starts the query
    # Assign last date to last_load_date variable
    for row in read_query:
        logging.info(f'{job_name}: Last load into {table_name} was '
                     f'for {row.last_load_date}')
        return row.last_load_date
    return None


def query_to_dataframe(project_name, sql_script, job_name):
    logging.info(f'{job_name}: Starting query run to dataframe')
    client = bigquery.Client(project=project_name)
    sql = sql_script
    dataframe = client.query(sql).to_dataframe()
    logging.info(f'{job_name}: Read complete and data stored in dataframe="dataframe"')
    return dataframe


if __name__ == '__main__':
    print("Nothing to Run")
