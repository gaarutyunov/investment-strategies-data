# Equities historical data fetching from OpenAPI Tinkoff

## Getting started

1. Rename config.example.yml to config.yml
2. Provide your Tinkoff OpenApi token in the file
3. Provide database connection string

```yaml
api:
  token: your_token

db:
  conn: postgresql://user:password@localhost:5432/dbname
```

### Database

1. Execute init.sql file to create database and tables

### Install dependencies

1. Execute in project root

```
go mod download
```

### Build

1. Execute in project root

```
go build
```

## Usage

### Parse and fetch data

1. Execute in project root

```
./investment-strategies-data
``` 