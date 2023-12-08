import omni.client

# server_url = "omniverse://simready.ov.nvidia.com"
server_url = "omniverse://localhost"

# Attempt to access a resource on the server
status, response = omni.client.stat(server_url)

if status == omni.client.Result.OK:
    print(f"Successfully connected to the server {server_url}")
else:
    print(f"Failed to connect to the server {server_url}.")
