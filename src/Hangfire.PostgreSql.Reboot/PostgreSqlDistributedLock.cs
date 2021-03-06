// This file is part of Hangfire.PostgreSql.Reboot
// Copyright � 2016 Mihai Bogdan Eugen.
// 
// Hangfire.PostgreSql is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.PostgreSql  is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the works of Frank Hommers and Sergey Odinokov, the author of Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to them.

using System;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Dapper;

namespace Hangfire.PostgreSql.Reboot
{
    internal class PostgreSqlDistributedLock : IDisposable
    {
        private readonly IDbConnection _connection;
        private readonly string _resource;
        private readonly PostgreSqlStorageOptions _options;
        private bool _completed;

        public PostgreSqlDistributedLock(string resource, TimeSpan timeout, IDbConnection connection,
            PostgreSqlStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource)) throw new ArgumentNullException(nameof(resource));
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (options == null) throw new ArgumentNullException(nameof(options));

            _resource = resource;
            _connection = connection;
            _options = options;

            if (_options.UseNativeDatabaseTransactions)
                PostgreSqlDistributedLock_Init_Transaction(resource, timeout, connection, options);
            else
                PostgreSqlDistributedLock_Init_UpdateCount(resource, timeout, connection, options);
        }

        private static void PostgreSqlDistributedLock_Init_Transaction(string resource, TimeSpan timeout, IDbConnection connection, PostgreSqlStorageOptions options)
        {
            var lockAcquiringTime = new Stopwatch();
            lockAcquiringTime.Start();

            var tryAcquireLock = true;

            while (tryAcquireLock)
            {
                try
                {
                    int rowsAffected;
                    using (var trx = connection.BeginTransaction(IsolationLevel.RepeatableRead))
                    {
                        rowsAffected = connection.Execute(@"
INSERT INTO """ + options.SchemaName + @""".""lock""(""resource"") 
SELECT @resource
WHERE NOT EXISTS (
    SELECT 1 FROM """ + options.SchemaName + @""".""lock"" 
    WHERE ""resource"" = @resource
);
", new
                        {
                            resource
                        }, trx);
                        trx.Commit();
                    }
                    if (rowsAffected > 0) return;
                }
                catch (Exception)
                {
                    // ignored
                }

                if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
                    tryAcquireLock = false;
                else
                {
                    int sleepDuration = (int) (timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                        Thread.Sleep(sleepDuration);
                    else
                        tryAcquireLock = false;
                }
            }

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource '{resource}': {"Lock timeout"}.");
        }

        private static void PostgreSqlDistributedLock_Init_UpdateCount(string resource, TimeSpan timeout, IDbConnection connection, PostgreSqlStorageOptions options)
        {
            var lockAcquiringTime = new Stopwatch();
            lockAcquiringTime.Start();

            var tryAcquireLock = true;

            while (tryAcquireLock)
            {
                try
                {
                    connection.Execute(@"
INSERT INTO """ + options.SchemaName + @""".""lock""(""resource"", ""updatecount"") 
SELECT @resource, 0
WHERE NOT EXISTS (
    SELECT 1 FROM """ + options.SchemaName + @""".""lock"" 
    WHERE ""resource"" = @resource
);
", new
 {
     resource
 });
                }
                catch (Exception)
                {
                    // ignored
                }

                var rowsAffected = connection.Execute(@"UPDATE """ + options.SchemaName + @""".""lock"" SET ""updatecount"" = 1 WHERE ""updatecount"" = 0");

                if (rowsAffected > 0) return;

                if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
                    tryAcquireLock = false;
                else
                {
                    int sleepDuration = (int)(timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                        Thread.Sleep(sleepDuration);
                    else
                        tryAcquireLock = false;
                }
            }

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource '{resource}': {"Lock timeout"}.");
        }

        public void Dispose()
        {
            if (_completed) return;

            _completed = true;

            var rowsAffected = _connection.Execute(@"
DELETE FROM """ + _options.SchemaName + @""".""lock"" 
WHERE ""resource"" = @resource;
", new
            {
                resource = _resource
            });


            if (rowsAffected <= 0)
            {
                throw new PostgreSqlDistributedLockException(
                    $"Could not release a lock on the resource '{this._resource}'. Lock does not exists.");
            }
        }
    }
}