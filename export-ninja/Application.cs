using System.CommandLine;
using System.Data;
using System.Data.Common;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
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

            var rootCommand = new RootCommand("Export Ninja - Export given tables to JSON Lines files");
            rootCommand.AddOption(tableNameOption);
            rootCommand.AddOption(fileNamePrefixOption);
            rootCommand.AddOption(databaseTypeOption);

            rootCommand.SetHandler(async (context) =>
            {
                var tableNameOptionValues = context.ParseResult.GetValueForOption<string[]>(tableNameOption);
                var fileNamePrefixOptionValue = context.ParseResult.GetValueForOption(fileNamePrefixOption);
                var databaseType = context.ParseResult.GetValueForOption(databaseTypeOption);

                var token = context.GetCancellationToken();
                returnCode = await RunApplicationAsync(tableNameOptionValues, fileNamePrefixOptionValue, databaseType, token);
            });

            await rootCommand.InvokeAsync(args);

            return returnCode;
        }

        private async Task<int> RunApplicationAsync(string[] tableNameArg, string? fileNamePrefixArg, string? databaseType, CancellationToken cancellationToken)
        {
            factory = DbProviderFactories.GetFactory(databaseType);

            var parallelOptions = new ParallelOptions()
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = Environment.ProcessorCount
            };

            var exportFolder = Path.Join(AppDomain.CurrentDomain.BaseDirectory, "exports");

            Directory.CreateDirectory(exportFolder);

            try
            {
                await Parallel.ForEachAsync(tableNameArg, cancellationToken, async (tableName, cancellationToken) =>
                {
                    Log.Information($"Start exporting {tableName}");

                    var filePath = Path.Join(exportFolder, $"{fileNamePrefixArg ?? fileNamePrefixArg + "_"}{tableName}-{DateTime.UtcNow:yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'}.jsonl");

                    using (var connection = factory.CreateConnection())
                    {
                        if (connection == null)
                        {
                            throw new InvalidOperationException("Can not create connection to database.");
                        }

                        connection.ConnectionString = _config.GetConnectionString("Database");
                        await connection.OpenAsync();

                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = "SELECT * FROM @TableName";
                            command.CommandType = CommandType.Text;

                            var parameter = command.CreateParameter();
                            parameter.ParameterName = "@TableName";
                            parameter.Value = tableName;
                            parameter.DbType = DbType.String;
                            command.Parameters.Add(parameter);

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

                                            await file.WriteLineAsync(JsonSerializer.Serialize(tmpObj));
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