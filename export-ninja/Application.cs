using System.CommandLine;
using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using Serilog;

namespace ExportNinja
{
    public class Application
    {
        private readonly IConfiguration _config;

        private DbProviderFactory factory;

        private List<int> oracleErrorsToSoftFail = new List<int>() { 942 };

        private List<int> mysqlErrorsToSoftFail = new List<int> { 1146 };

        public Application(IConfiguration config)
        {
            _config = config;
        }

        // This is where the arguments are defined
        public async Task<int> ExecuteAsync(string[] args)
        {
            int returnCode = 0;

            var tableNameOption = new Option<string[]>("--table")
            {
                Description = "Table name(s) [space seperated]",
                AllowMultipleArgumentsPerToken = true,
                IsRequired = true
            };

            var fileNamePrefixOption = new Option<string>("--fileNamePrefix")
            {
                Description = "File name prefix.",
                IsRequired = false
            };

            var databaseTypeOption = new Option<string>("--type")
            {
                Description = "Database type",
                IsRequired = true
            }.FromAmong("mysql", "oracle");

            var fileExportPathOption = new Option<string>("--path")
            {
                Description = "Path where files should be stored. [Default: ./exports]",
                IsRequired = false
            };

            var connectionStringOption = new Option<string>("--connectionString")
            {
                Description = "DB connection string [You can also use appsettings.json]",
                IsRequired = false
            };

            var withTimeStampOption = new Option<bool>("--withTimeStamp")
            {
                Description = "Option to add a time stamp suffix to exported files",
                IsRequired = false
            };

            var softFailTableNotFound = new Option<bool>("--softFail")
            {
                Description = "Only a warning is shown, when given table not found in DB. Skipping.",
                IsRequired = false
            };

            var rootCommand = new RootCommand("Export Ninja - Export given tables to JSON Lines files");
            rootCommand.AddOption(tableNameOption);
            rootCommand.AddOption(fileNamePrefixOption);
            rootCommand.AddOption(databaseTypeOption);
            rootCommand.AddOption(fileExportPathOption);
            rootCommand.AddOption(connectionStringOption);
            rootCommand.AddOption(withTimeStampOption);
            rootCommand.AddOption(softFailTableNotFound);

            rootCommand.SetHandler(async (context) =>
            {
                var tableNameOptionValues = context.ParseResult.GetValueForOption(tableNameOption);
                var fileNamePrefixOptionValue = context.ParseResult.GetValueForOption(fileNamePrefixOption);
                var databaseTypeOptionValue = context.ParseResult.GetValueForOption(databaseTypeOption);
                var fileExportPathOptionValue = context.ParseResult.GetValueForOption(fileExportPathOption);
                var connectionStringOptionValue = context.ParseResult.GetValueForOption(connectionStringOption);
                var withTimeStampOptionValue = context.ParseResult.GetValueForOption(withTimeStampOption);
                var softFailTableNotFoundValue = context.ParseResult.GetValueForOption(softFailTableNotFound);

                var ct = context.GetCancellationToken();
                returnCode = await RunApplicationAsync(
                    tableNameOptionValues,
                    fileNamePrefixOptionValue,
                    databaseTypeOptionValue,
                    fileExportPathOptionValue,
                    connectionStringOptionValue,
                    withTimeStampOptionValue,
                    softFailTableNotFoundValue,
                    ct);
            });

            await rootCommand.InvokeAsync(args);

            return returnCode;
        }

        private void HandleException(Exception ex, string tableName, bool softFailTableNotFound)
        {
            if (ex is OracleException)
            {
                var oEx = ex as OracleException;
                if (softFailTableNotFound && oracleErrorsToSoftFail.Contains(oEx.Number))
                {
                    Log.Warning($"Warning: Soft fail. Table: {tableName} Msg: {oEx.Message}");
                }
                else
                {
                    throw ex;
                }
            }

            if (ex is MySqlException)
            {
                var mEx = ex as MySqlException;
                if (softFailTableNotFound && mysqlErrorsToSoftFail.Contains(mEx.Number))
                {
                    Log.Warning($"Warning: Soft fail. Table: {tableName} Msg: {mEx.Message}");
                }
                else
                {
                    throw ex;
                }
            }
        }

        private async Task<int> RunApplicationAsync(
            string[]? tableNameArg,
            string? fileNamePrefixArg,
            string? databaseType,
            string? exportPath,
            string? connectionString,
            bool withTimeStamp,
            bool? softFailTableNotFound,
            CancellationToken cancellationToken)
        {
            if(tableNameArg == null || databaseType == null)
            {
                throw new InvalidDataException("Please provide table name or database type");
            }

            factory = DbProviderFactories.GetFactory(databaseType);

            var parallelOptions = new ParallelOptions()
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = Environment.ProcessorCount
            };

            var defaultExportFolder = Path.Join(AppDomain.CurrentDomain.BaseDirectory, "exports");
            var exportFolder = exportPath ?? defaultExportFolder;

            Directory.CreateDirectory(exportFolder);

            try
            {
                await Parallel.ForEachAsync(tableNameArg, cancellationToken, async (tableName, cancellationToken) =>
                {
                    try
                    {
                        Log.Information($"Start exporting {tableName}");

                        var builder = factory.CreateCommandBuilder();
                        string escapedTableName = builder.QuoteIdentifier(tableName);

                        var fileName = tableName;
                        if (fileNamePrefixArg != null)
                        {
                            fileName = fileNamePrefixArg + "_" + tableName;
                        }

                        if (withTimeStamp)
                        {
                            fileName = $"{fileName}_{DateTime.UtcNow:yyyyMMddTHHmmss}";
                        }

                        var filePath = Path.Join(exportFolder, $"{fileName}.jsonl");

                        using (var connection = factory.CreateConnection())
                        {
                            if (connection == null)
                            {
                                throw new InvalidOperationException("Can not create connection to database.");
                            }

                            connection.ConnectionString = connectionString ?? _config.GetConnectionString("Database");
                            await connection.OpenAsync();

                            using (var command = connection.CreateCommand())
                            {
                                command.CommandText = $@"SELECT * FROM {escapedTableName}";

                                using (var reader = await command.ExecuteReaderAsync(CommandBehavior.Default))
                                {
                                    using (var file = File.CreateText(filePath))
                                    {
                                        var tmpObj = new Dictionary<string, object>();
                                        string columnName;
                                        object columnValue;

                                        while (await reader.ReadAsync(cancellationToken))
                                        {
                                            if (reader.HasRows)
                                            {
                                                for (int i = 0; i < reader.FieldCount; i++)
                                                {
                                                    columnName = reader.GetName(i);
                                                    columnValue = reader.GetValue(i);
                                                    tmpObj[columnName] = columnValue;
                                                }

                                                await file.WriteLineAsync(JsonConvert.SerializeObject(tmpObj));
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        Log.Information($"DONE: exporting {tableName} to {filePath}");
                    }
                    catch (Exception ex)
                    {
                        HandleException(ex, tableName, softFailTableNotFound ?? false);
                    }
                });

                return 0;
            }
            catch (Exception ex)
            {
                Log.Error("ERROR: {0}", ex.Message);

                return 1;
            }
        }
    }
}