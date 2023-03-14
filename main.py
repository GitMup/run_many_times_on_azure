import os
import sys
import configparser
import datetime
import azure.storage.blob
from prefect import flow, task, unmapped, get_run_logger
from prefect_dask.task_runners import DaskTaskRunner
from azure.identity import DefaultAzureCredential
from azure_identity_credential_adapter import AzureIdentityCredentialAdapter
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
    generate_container_sas,
    ContainerSasPermissions,
)
import azure.batch as batch
import azure.batch.models as batchmodels


def read_config(path: str):
    """You pass butter
    :param path:
    :return:
    """
    config = configparser.ConfigParser()
    config.read(path)
    return config


@task(name="Create azure storage container")
def create_containers(blob_service_client: BlobServiceClient, names: list):
    """You pass butter

    :param blob_service_client:
    :param names:
    :return:
    """
    for name in names:
        try:
            blob_service_client.create_container(name)
        except ResourceExistsError:
            pass


def create_blob_service_client(config: configparser.ConfigParser):
    """You pass butter

    :param config:
    :return:
    """
    return BlobServiceClient(
        account_url=f"https://{config['storage_account']['name']}.{config['storage_account']['domain']}/",
        credential=config["storage_account"]["key"],
    )


def upload_file_to_container(
    blob_service_client: BlobServiceClient,
    config: configparser.ConfigParser,
    container_name: str,
    file_path: str,
    identifier: str,
):
    """

    :param blob_service_client:
    :param config:
    :param container_name:
    :param file_path:
    :param identifier:
    :return:
    """
    blob_name = os.path.basename(file_path)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)
    if not blob_client.exists():
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=False)

    # Obtain the SAS token for the container.
    sas_token = generate_blob_sas(
        config["storage_account"]["name"],
        container_name,
        blob_name,
        account_key=config["storage_account"]["key"],
        permission=BlobSasPermissions(read=True),
        expiry=datetime.datetime.utcnow()
        + datetime.timedelta(float(config["general"]["time_out_hrs"])),
    )
    return identifier, batchmodels.ResourceFile(
        file_path=blob_name,
        http_url=f"https://{config['storage_account']['name']}."
        f"{config['storage_account']['domain']}/"
        f"{container_name}/"
        f"{blob_name}?"
        f"{sas_token}",
    )


@task
def upload_files(config, container_name, file_paths: list, identifiers: list):
    blob_service_client = create_blob_service_client(config)
    return dict(
        [
            upload_file_to_container(
                blob_service_client,
                config,
                container_name,
                file_path,
                identifier,
            )
            for file_path, identifier in zip(file_paths, identifiers)
        ]
    )


def create_batch_service_client(config: configparser.ConfigParser):
    credential = AzureIdentityCredentialAdapter(
        DefaultAzureCredential(exclude_interactive_browser_credential=True),
        resource_id="https://batch.core.windows.net//.default",
    )
    return batch.BatchServiceClient(
        credential, batch_url=config["batch_account"]["url"]
    )


@task
def create_pool(config: configparser.ConfigParser):
    """Create a pool of nodes to run models on.

    :param config:
    :param batch_service_client:
    :return:
    """
    batch_service_client = create_batch_service_client(config)
    if not batch_service_client.pool.exists(config["meta"]["project_name"]):
        batch_service_client.pool.add(
            batch.models.PoolAddParameter(
                id=config["meta"]["project_name"],
                virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                    image_reference=batchmodels.ImageReference(
                        publisher="Canonical",
                        offer="0001-com-ubuntu-server-focal",
                        sku="20_04-lts-gen2",
                        version="latest",
                    ),
                    node_agent_sku_id="batch.node.ubuntu 20.04",
                ),
                vm_size=config["pool"]["size"],
                target_dedicated_nodes=config["pool"]["count"],
                target_low_priority_nodes=config["pool"]["low_priority_nodes"],
                start_task=batchmodels.StartTask(
                    command_line='/bin/bash -c "sudo apt-get update &&'
                    " sudo apt-get install -y python3-pip unzip &&"
                    ' pip3 install pandas numpy rasterio"',
                    wait_for_success=True,
                    user_identity=batchmodels.UserIdentity(
                        auto_user=batchmodels.AutoUserSpecification(
                            scope=batchmodels.AutoUserScope.pool,
                            elevation_level=batchmodels.ElevationLevel.admin,
                        )
                    ),
                ),
            )
        )
    else:
        get_run_logger().info("Pool exists, continuing...")


@task
def create_job(config: configparser.ConfigParser):
    """

    :param config:
    :param batch_service_client:
    :return:
    """
    batch_service_client = create_batch_service_client(config)
    job = batch.models.JobAddParameter(
        id=config["meta"]["project_name"],
        pool_info=batch.models.PoolInformation(pool_id=config["meta"]["project_name"]),
    )
    try:
        batch_service_client.job.add(job)
    except batchmodels.BatchErrorException:
        batch_service_client.job.delete(job)
        batch_service_client.job.add(job)


def list_output_blob_names(config: configparser.ConfigParser, identifier: str):
    """Make a list of model output files which are to be uploaded to output container.

    :param config:
    :param identifier:
    :return:
    """
    return [
        s.format(identifier=identifier)
        for s in list(config["output_file_patterns"].values())
    ]


def build_command(
    config: configparser.ConfigParser,
    shared_files: dict[str : batchmodels.ResourceFile],
    parameter_file: tuple[str : batchmodels.ResourceFile],
):
    """Build command for a single model run.
    In this example model uses two spatial maps and a parameter file.

    :param config:
    :param parameter_file:
    :param shared_files:
    :return:
    """

    return " && ".join(list(config["command"].values())).format(
        shared_files=shared_files, parameter_file=parameter_file
    )


def build_container_sas_url(config: configparser.ConfigParser, container_name: str):
    """Create sas url for use when uploading model output to output container.

    :param config:
    :param container_name:
    :return:
    """
    sas_token = generate_container_sas(
        config["storage_account"]["name"],
        container_name,
        account_key=config["storage_account"]["key"],
        permission=ContainerSasPermissions(write=True),
        expiry=datetime.datetime.utcnow()
        + datetime.timedelta(hours=float(config["general"]["time_out_hrs"])),
    )
    return (
        f"https://{config['storage_account']['name']}."
        f"{config['storage_account']['domain']}/"
        f"{container_name}?"
        f"{sas_token}"
    )


@task(name="Add model run to job as task")
def add_model_runs(
    config: configparser.ConfigParser,
    shared_files: dict[str : batchmodels.ResourceFile],
    parameter_files: dict[str : batchmodels.ResourceFile],
):
    """Add a model run as task to the azure batch job. The task is named after the parameter file.
    Takes all output files that are in output_blob_names list and upload them to to the container of the given container
    sas url.

    :param parameter_files:
    :param batch_service_client:
    :param config:
    :param shared_files:
    :return:
    """
    batch_service_client = create_batch_service_client(config)
    for parameter_file in parameter_files.items():

        # add model run to job
        batch_service_client.task.add(
            config["meta"]["project_name"],
            batch.models.TaskAddParameter(
                id=parameter_file[0],
                command_line=build_command(config, shared_files, parameter_file),
                resource_files=list(shared_files.values()) + [parameter_file[1]],
                output_files=[
                    batchmodels.OutputFile(
                        file_pattern=file_pattern,
                        destination=batchmodels.OutputFileDestination(
                            container=batchmodels.OutputFileBlobContainerDestination(
                                container_url=build_container_sas_url(
                                    config, f"output-{parameter_file[0]}"
                                )
                            )
                        ),
                        upload_options=batchmodels.OutputFileUploadOptions(
                            upload_condition=batchmodels.OutputFileUploadCondition.task_success
                        ),
                    )
                    for file_pattern in list_output_blob_names(
                        config, parameter_file[0]
                    )
                ],
            ),
        )


def download_container(
    config: configparser.ConfigParser,
    blob_service_client: azure.storage.blob.BlobServiceClient,
    container_name: str,
):
    """Download all files in an azure blob storage container to local system.

    :param config:
    :param blob_service_client:
    :param container_name:
    :param output_blob_names:
    :return:
    """
    run_id = container_name.split("-")[1]
    output_folder = f"outputs/{run_id}"
    try:
        os.makedirs(output_folder)
    except FileExistsError:
        pass
    for blob_name in list_output_blob_names(config, run_id):
        if blob_service_client.get_blob_client(container_name, blob_name).exists():
            with open(os.path.join(output_folder, blob_name), "wb") as download_file:
                download_file.write(
                    blob_service_client.get_container_client(container=container_name)
                    .download_blob(blob_name)
                    .readall()
                )
        else:
            return False
    return True


@task
def wait_and_download(
    config: configparser.ConfigParser,
    task_ids: list,
):
    """Returns when all tasks in the specified job reach the Completed state.

    :param config:
    :param task_ids:
    :return failed downloads:
    """

    # instantiate service clients
    batch_service_client = create_batch_service_client(config)
    blob_service_client = create_blob_service_client(config)

    # instantiate number of tasks, success and failed list
    n_tasks = len(task_ids)
    successful_downloads = []
    failed_downloads = []

    # wait for task to complete and while waiting download container for each task that finishes
    timeout_expiration = datetime.datetime.now() + datetime.timedelta(
        hours=float(config["general"]["time_out_hrs"])
    )
    while datetime.datetime.now() < timeout_expiration:

        # then check if tasks are completed and download them if they have not been downloaded yet
        if len(successful_downloads + failed_downloads) < n_tasks:
            for task_id in task_ids:
                if (
                    (
                        batch_service_client.task.get(
                            config["meta"]["project_name"], task_id
                        ).state
                        == batchmodels.TaskState.completed
                    )
                    and task_id not in successful_downloads
                    and task_id not in failed_downloads
                ):
                    get_run_logger().info(f"{task_id} is completed, downloading...")
                    if download_container(
                        config, blob_service_client, f"output-{task_id}"
                    ):
                        successful_downloads.append(task_id)
                        blob_service_client.delete_container(f"output-{task_id}")
                    else:
                        failed_downloads.append(task_id)
        else:
            get_run_logger().info("All runs completed, hooray!")
            get_run_logger().info(
                f"Following runs failed to download: {failed_downloads}"
            )
            return failed_downloads


@task
def clean_up_resources(
    config: configparser.ConfigParser,
    containers: list,
):
    """Delete job, pool and all containers.

    :param containers:
    :param config:
    :param batch_service_client: BatchServiceClient object
    :param blob_service_client: BlockBlobService object
    :return: nothing
    """
    batch_service_client = create_batch_service_client(config)
    blob_service_client = create_blob_service_client(config)

    # Clean up Batch resources
    batch_service_client.pool.delete(config["meta"]["project_name"])
    for container_name in containers:
        try:
            blob_service_client.delete_container(container_name)
        except azure.core.exceptions.ResourceNotFoundError:
            get_run_logger().warning(f"Container {container_name} does not exist")


@flow(task_runner=DaskTaskRunner())
def run_many_times_on_azure(config_path):
    config, shared_file_paths = read_config(config_path)
    parameter_file_names = os.listdir(config["parameter_files"]["directory"])
    input_container_names = [
        f"{config['meta']['project_name']}-shared-files",
        f"{config['meta']['project_name']}-parameter-files",
    ]
    output_container_names = [
        f"output-{os.path.splitext(file)[0]}" for file in parameter_file_names
    ]

    # Create blob service client and upload shared and parameter files to azure storage blob
    blob_service_client = create_blob_service_client(config)
    containers_created = create_containers(
        blob_service_client, input_container_names + output_container_names
    )

    # upload shared files
    shared_files = upload_files(
        config,
        f"{config['meta']['project_name']}-shared-files",
        shared_file_paths.values(),
        shared_file_paths.keys(),
        wait_for=[containers_created],
    )

    # upload parameter files
    parameter_files = upload_files(
        config,
        f"{config['meta']['project_name']}-parameter-files",
        [
            f"{os.path.join(config['parameter_files']['directory'],file)}"
            for file in parameter_file_names
        ],
        [os.path.splitext(file)[0] for file in parameter_file_names],
        wait_for=[containers_created],
    )

    # Create batch service client, make a pool and add a job to it.
    pool = create_pool(config)
    job = create_job(config, wait_for=[pool])

    # Add model runs to job and wait for them to be completed
    model_runs = add_model_runs(
        config,
        shared_files,
        parameter_files,
        wait_for=[job],
    )
    run_subsets = [
        [i for i in range(0, len(parameter_file_names), 4)],
        [i for i in range(1, len(parameter_file_names), 4)],
        [i for i in range(2, len(parameter_file_names), 4)],
        [i for i in range(3, len(parameter_file_names) + 4, 4)],
    ]
    try:
        completed_runs = wait_and_download.map(
            unmapped(config),
            run_subsets,
            wait_for=[model_runs],
        )
        for run in completed_runs:
            get_run_logger().warning(f"Following runs failed: {run}")

        # Download output and clean up everything
        clean_up_resources(
            config,
            input_container_names,
            wait_for=[completed_runs],
        )
    except (batchmodels.BatchErrorException, azure.core.exceptions):
        create_batch_service_client(config).pool.delete(config["meta"]["project_name"])


if __name__ == "__main__":
    state = run_many_times_on_azure(sys.argv[1])
