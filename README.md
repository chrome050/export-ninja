# Export Ninja

CLI tool to export database tables to JSON Lines files - The .Net 8 way

## Description

Supported databases:
  * MySQL (<=8.3)
  * Oracle (>= 11.2)

## Getting Started

### Installing

Just call export-ninja binary

### Executing program

```
Description:
  Export Ninja - Export given tables to JSON Lines files

Usage:
  export-ninja [options]

Options:
  --table <table> (REQUIRED)             Table name(s) [space seperated]
  --fileNamePrefix <fileNamePrefix>      File name prefix.
  --type <mysql|oracle> (REQUIRED)       Database type
  --path <path>                          Path where files should be stored. [Default: ./exports]
  --connectionString <connectionString>  DB connection string [You can also use appsettings.json]
  --withTimeStamp                        Option to add a time stamp suffix to exported files
  --version                              Show version information
  -?, -h, --help                         Show help and usage information
```
