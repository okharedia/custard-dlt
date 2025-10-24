# Custard DLT

A data pipeline project using dlt (data load tool) to replicate PostgreSQL data to MotherDuck.

## Features

- **PostgreSQL Replication**: Replicate tables from PostgreSQL to MotherDuck
- **Schedule Agreements Generation**: Generate schedule agreements from iCal calendars
- **SSH Tunnel Support**: Securely connect to remote databases via SSH
- **Prefect Integration**: Workflow orchestration with Prefect

## Setup

### Prerequisites

- Python 3.11+
- A [MotherDuck](https://motherduck.com/) account and token
- Access to your PostgreSQL database

### Installation

1. Install dependencies:

```bash
uv pip install -e .
# or
pip install -r requirements.txt
```

2. Configure MotherDuck credentials:

Create or edit `.dlt/secrets.toml` and add your MotherDuck token:

```toml
[destination.motherduck.credentials]
database = "custard"  # Your MotherDuck database name
token = "YOUR_MOTHERDUCK_TOKEN"  # Get from https://app.motherduck.com/
```

3. Configure your data source credentials in `.dlt/secrets.toml`:

```toml
[sources.sql_database.credentials]
username = "your_db_username"
password = "your_db_password"

[ssh]
username = "your_ssh_username"
host = "your_ssh_host"
private_key = "your_ssh_private_key"
```

### Getting Your MotherDuck Token

1. Sign up or log in to [MotherDuck](https://motherduck.com/)
2. Go to [https://app.motherduck.com/](https://app.motherduck.com/)
3. Navigate to Settings → Tokens
4. Create a new token or copy your existing token
5. Add it to your `.dlt/secrets.toml` file

## Usage

### Replicate PostgreSQL Tables

```bash
python flows/replicate_postgres_raw.py
```

### Generate Schedule Agreements

```bash
python flows/generate_schedule_agreements.py
```

### Using with Prefect

The flows are decorated with Prefect decorators and can be deployed using Prefect:

```bash
prefect deploy --all
```

## Project Structure

```
custard-dlt/
├── flows/
│   ├── replicate_postgres_raw.py      # Main replication flow
│   ├── generate_schedule_agreements.py # Schedule generation flow
│   └── utils/
│       ├── pkey.py                     # SSH key utilities
│       └── ssh_tunnel.py               # SSH tunnel management
├── get_events_between.py               # iCal event parser
├── .dlt/
│   └── secrets.toml                    # Configuration (git-ignored)
├── pyproject.toml                      # Project dependencies
└── README.md                           # This file
```

## Migration from DuckDB

If you were previously using local DuckDB, your data has been migrated to MotherDuck (cloud-based DuckDB). The local `custard.duckdb` file is no longer needed but is kept for backup purposes.

Benefits of MotherDuck:
- Cloud-based storage
- Accessible from anywhere
- Better collaboration
- Automatic backups
- Same DuckDB SQL interface

## Development

### Adding New Data Sources

To add a new data source, create a new flow in the `flows/` directory following the pattern in existing flows.

### Configuration

All sensitive configuration should go in `.dlt/secrets.toml` (git-ignored). Non-sensitive configuration can go in `.dlt/config.toml`.

## License

See LICENSE file for details.

