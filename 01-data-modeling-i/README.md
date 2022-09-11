# Data Modeling I

## Getting Started

```sh
cd 01-data-modeling-i
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

### Prerequisite when install psycopg2 package

For Debian/Ubuntu users:

```sh
sudo apt install -y libpq-dev
```

For Mac users:

```sh
brew install postgresql
```

## Running Postgres

```sh
docker-compose up
```

```open browser to connect postgres and login: http://localhost:8080/
 - System: PostgreSQL
 - Server: postgres
 - Username: postgres
 - Password: postgres
 - Database: postgres
```

To shutdown, press Ctrl+C and run:

```sh
docker-compose down
```

## Running ETL Scripts

```sh
python create_tables.py
python etl.py
```