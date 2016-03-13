using System;
using Dapper;
using Hangfire.PostgreSql.Reboot;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlInstallerFacts
    {

        [Fact]
        public void InstallingSchemaShouldNotThrowAnException()
        {
            UseConnection(connection =>
            {
                var schemaName = "hangfire_tests_" + Guid.NewGuid().ToString().Replace("-", "_").ToLower();

                PostgreSqlObjectsInstaller.Install(connection, schemaName);

                connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
            });
        }


        private static void UseConnection(Action<NpgsqlConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
}
