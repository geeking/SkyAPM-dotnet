using Microsoft.Extensions.DependencyInjection;
using SkyApm.Utilities.DependencyInjection;
using System;

namespace SkyApm.Diagnostics.MySql
{
    public static class SkyWalkingBuilderExtensions
    {
        public static SkyApmExtensions AddMySql(this SkyApmExtensions extensions)
        {
            if (extensions == null)
            {
                throw new ArgumentNullException(nameof(extensions));
            }

            extensions.Services.AddSingleton<ITracingDiagnosticProcessor, MySqlTracingDiagnosticProcessor>();

            return extensions;
        }
    }
}