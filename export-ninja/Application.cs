using System.CommandLine;
using System.Data;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Serilog;

namespace ExportNinja
{
    public class Application
    {
        private readonly IConfiguration _config;

        public Application(IConfiguration config)
        {
            _config = config;
        }

        // This is where the arguments are defined
        public async Task<int> ExecuteAsync(string[] args)
        {
            int returnCode = 0;

            var tableNameOption = new Option<string>("--table")
            {
                Description = "Table name",
                IsRequired = true
            };

            var fileNameOption = new Option<string>("--fileName")
            {
                Description = "File name. If not provided, table name is used.",
                IsRequired = false
            };

            var rootCommand = new RootCommand("Export Ninja - Export given table to JSON Lines file");
            rootCommand.AddOption(tableNameOption);
            rootCommand.AddOption(fileNameOption);

            rootCommand.SetHandler(async (context) =>
            {
                var tableNameOptionValue = context.ParseResult.GetValueForOption(tableNameOption);
                var fileNameOptionValue = context.ParseResult.GetValueForOption(fileNameOption);

                var token = context.GetCancellationToken();
                returnCode = await RunApplicationAsync(tableNameOptionValue, fileNameOptionValue, token);
            });

            await rootCommand.InvokeAsync(args);

            return returnCode;
        }

        private async Task<int> RunApplicationAsync(string? tableNameArg, string? fileNameArg, CancellationToken cancellationToken)
        {
            Log.Information($"Start exporting {tableNameArg}");

            var exportFolder = Path.Join(AppDomain.CurrentDomain.BaseDirectory, "exports");

            Directory.CreateDirectory(exportFolder);

            var filePath = Path.Join(exportFolder, $"{fileNameArg ?? tableNameArg}-{DateTime.UtcNow:yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'}.jsonl");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(_config.GetConnectionString("Database")))
                {
                    await connection.OpenAsync();

                    using (MySqlCommand command = new MySqlCommand($"SELECT * FROM {tableNameArg}", connection))
                    {
                        using (MySqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.Default))
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

                Log.Information($"DONE: exporting {tableNameArg} to {filePath}");

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