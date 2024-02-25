using ExportNinja;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((hostingContext, config) =>
    {
        config.AddJsonFile("appsettings.json", false);
    })
    .ConfigureServices((context, services) =>
    {
        services.AddSingleton<Application>();
    })
    .UseSerilog((context, lc) => lc.ReadFrom.Configuration(context.Configuration))
  .Build();

// Run the application
var application = host.Services.GetRequiredService<Application>();

await application.ExecuteAsync(args);