from sshtunnel import SSHTunnelForwarder
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE


@task(retries=1, retry_delay_seconds=1, cache_policy=NO_CACHE)
def create_ssh_tunnel(ssh_host, ssh_port, ssh_username, pkey, remote_db_host, remote_db_port):
    """Establishes an SSH tunnel and returns the tunnel object."""
    tunnel = SSHTunnelForwarder(
        (ssh_host, int(ssh_port)),
        ssh_username=ssh_username,
        ssh_pkey=pkey,
        remote_bind_address=(remote_db_host, remote_db_port),
    )
    tunnel.start()
    get_run_logger().info("SSH tunnel started.")
    return tunnel
