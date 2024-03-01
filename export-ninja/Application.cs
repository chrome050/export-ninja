using System.CommandLine;
using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Serilog;

namespace ExportNinja
{
    public class Application
    {
        private readonly IConfiguration _config;

        private DbProviderFactory factory;

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

            var rootCommand = new RootCommand("Export Ninja - Export given tables to JSON Lines files");
            rootCommand.AddOption(tableNameOption);
            rootCommand.AddOption(fileNamePrefixOption);
            rootCommand.AddOption(databaseTypeOption);
            rootCommand.AddOption(fileExportPathOption);
            rootCommand.AddOption(connectionStringOption);
            rootCommand.AddOption(withTimeStampOption);

            rootCommand.SetHandler(async (context) =>
            {
                var tableNameOptionValues = context.ParseResult.GetValueForOption(tableNameOption);
                var fileNamePrefixOptionValue = context.ParseResult.GetValueForOption(fileNamePrefixOption);
                var databaseTypeOptionValue = context.ParseResult.GetValueForOption(databaseTypeOption);
                var fileExportPathOptionValue = context.ParseResult.GetValueForOption(fileExportPathOption);
                var connectionStringOptionValue = context.ParseResult.GetValueForOption(connectionStringOption);
                var withTimeStampOptionValue = context.ParseResult.GetValueForOption(withTimeStampOption);

                var token = context.GetCancellationToken();
                returnCode = await RunApplicationAsync(tableNameOptionValues, fileNamePrefixOptionValue, databaseTypeOptionValue, fileExportPathOptionValue, connectionStringOptionValue, withTimeStampOptionValue, token);
            });

            await rootCommand.InvokeAsync(args);

            return returnCode;
        }

        private async Task<int> RunApplicationAsync(string[]? tableNameArg, string? fileNamePrefixArg, string? databaseType, string? exportPath, string? connectionString, bool withTimeStamp, CancellationToken cancellationToken)
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
                    Log.Information($"Start exporting {tableName}");

                    var builder = factory.CreateCommandBuilder();
                    string escapedTableName = builder.QuoteIdentifier(tableName);

                    var fileName = tableName;
                    if (fileNamePrefixArg != null)
                    {
                        fileName = fileNamePrefixArg + "_" + tableName;
                    }

                    if(withTimeStamp)
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
                });

                return 0;
            }
            catch (Exception ex)
            {
                Log.Error("ERROR: {0} {1}", ex.Message, ex);

                return 1;
            }
        }
    }
}