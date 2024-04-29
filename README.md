# Export Ninja

CLI tool to export database tables to JSON Lines files - The .Net way

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
  --table <table> (REQUIRED)             Table name(s) with optional file name (by adding :<fileName> after the table
                                         name) [space seperated]
  --fileNamePrefix <fileNamePrefix>      File name prefix.
  --type <mysql|oracle> (REQUIRED)       Database type
  --path <path>                          Path where files should be stored. [Default: ./exports]
  --connectionString <connectionString>  DB connection string [You can also use appsettings.json]
  --withTimeStamp                        Option to add a time stamp suffix to exported files
  --softFail                             Only a warning is shown, when given table is not found in DB. Skipping.
  --tnsAdminPath                         Oracle TNS_ADMIN path. [Only used for Oracle]
  --version                              Show version information
  -?, -h, --help                         Show help and usage information
```

### Naming of exported files

If the table name and no other argument such as "--fileNamePrefix" or "--withTimeStamp" is specified, the table name is used as the file name. However, if "--fileNamePrefix" is added, the specified prefix is written before the table name. With "--withTimeStamp", a timestamp (UTC) is added to the end of the table name.

If a separate file name is to be assigned for each table, this can be done with the addition ":<fileName>" when specifying a table name. Example: --table foo:bar baz:qux