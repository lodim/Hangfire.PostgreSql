using System;
using System.Web;
using Hangfire;
using Hangfire.Dashboard;
using Hangfire.PostgreSql.Reboot;
using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(Example.WebApp.Startup))]
namespace Example.WebApp
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
            ConfigureHangfire(app);
        }

        private static void ConfigureHangfire(IAppBuilder app)
        {
            var connectionString = ConfigurationService.GetConnectionString("DefaultConnection");
            GlobalConfiguration.Configuration.UseStorage(new PostgreSqlStorage(connectionString, new PostgreSqlStorageOptions
            {
                UseConnectionPooling = true,
            }));
            //GlobalConfiguration.Configuration.UseNLogLogProvider();
            app.UseHangfireDashboard("/hangfire", new DashboardOptions
            {
                //AuthorizationFilters = new[] { new AuthorizationFilter { Roles = "Administrator" }, },
                AppPath = VirtualPathUtility.ToAbsolute("~")
            });
            app.UseHangfireServer(new BackgroundJobServerOptions
            {
                WorkerCount = Environment.ProcessorCount,
                Queues = new[] { "critical", "default" }
            });
        }
    }
}
