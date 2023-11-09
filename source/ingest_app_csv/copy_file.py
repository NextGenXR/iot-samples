import omni.client
import asyncio
import os


async def connect_to_nucleus(login_url):
    result, _ = omni.client.connect(login_url)
    if result == omni.client.Result.OK:
        print(f"Connected to the Nucleus server {login_url}.")
    else:
        print(f"Failed to connect to {login_url}. Result: {result}")

    return result


async def check_connection_status(nucleus_server):
    status, _ = await omni.client.stat_async(nucleus_server)

    if status == omni.client.Result.OK:
        print(f"Connection is OK to {nucleus_server}.")
    elif status == omni.client.Result.ERROR_NOT_FOUND:
        print("Operation failed, {nucleus_server} not found.")
    else:
        print(f"Operation failed - possible connection issue with {nucleus_server}. Status: {status}")

    return status


async def path_exists(path):
    status, _ = await omni.client.stat_async(path)
    return status == omni.client.Result.OK


async def ensure_directory_exists(nucleus_server_path):
    print(f"Checking path: {nucleus_server_path}")

    status = await omni.client.create_folder_async(nucleus_server_path)

    if status == omni.client.Result.OK:
        # If the operation is successful or the folder already exists,
        #   the directory should be there
        print(f"Directory {nucleus_server_path} creation succeeded.")
        # Optionally, you can delete the temp file here
    elif status == omni.client.Result.ERROR_ALREADY_EXISTS:
        print(f"Directory {nucleus_server_path} already exists on the server.")
        return omni.client.Result.OK
    else:
        print(f"Failed to ensure directory exists. Status: {status}")

    return status


async def file_exists(path):
    status, _ = await omni.client.stat_async(path)
    return status


async def copy_file(local_file_path, server_file_path, nucleus_server_path):
    # Extracting the folder path from the full file path
    folder_path = '/'.join(nucleus_server_path.split('/')[:-1])

    await ensure_directory_exists(folder_path)
    filespec = f"file:{local_file_path}"

    # await asyncio.sleep(1)
    print(f"Copying {filespec} to {server_file_path}")

    # if not await path_exists(server_file_path):
    copy_status = await omni.client.copy_async(
        filespec, server_file_path,
        behavior=omni.client.CopyBehavior.OVERWRITE,
        message="Copy Conveyor Belt")
    if copy_status == omni.client.Result.OK:
        print(f"File {server_file_path} copied successfully!")
    else:
        print(f"Failed to copy {filespec} to {server_file_path}.")
        print(f"\tStatus: {copy_status}")


async def main():
    local_file = 'ConveyorBelt_A08_PR_NVD_01.usd'
    local_folder = 'G:\\GitLab\\iot-samples\\content'
    local_file_path = os.path.join(local_folder, local_file)
    # nucleus_server = 'omniverse://localhost'
    nucleus_server = 'omniverse://simready.ov.nvidia.com'
    server_path = 'Projects/IoT/Samples/HeadlessApp/'
    nucleus_server_path = f'{nucleus_server}/{server_path}'
    server_file_path = f'{nucleus_server_path}{local_file}'

    # await connect_to_nucleus(nucleus_server)

    # if login_result is not omni.client.ConnectionStatus.OK:
    #  return

    # Check and print the connection status
    await check_connection_status(nucleus_server)

    # if result is not omni.client.Result.OK:
    #    print("Not connected to the Nucleus server. Exiting.")
    #    return

    await copy_file(local_file_path, server_file_path, nucleus_server_path)

# Check if the script is run directly
if __name__ == "__main__":
    asyncio.run(main())
