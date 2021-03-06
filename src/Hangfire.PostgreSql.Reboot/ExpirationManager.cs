﻿// This file is part of Hangfire.PostgreSql.Reboot
// Copyright © 2016 Mihai Bogdan Eugen.
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
using System.Globalization;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.PostgreSql.Reboot
{
    internal class ExpirationManager : IServerComponent
    {
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private static readonly string[] ProcessedTables =
        {
            "counter",
            "job",
            "list",
            "set",
            "hash",
        };

        private readonly PostgreSqlStorage _storage;
        private readonly TimeSpan _checkInterval;
        private readonly PostgreSqlStorageOptions _options;

        public ExpirationManager(PostgreSqlStorage storage, PostgreSqlStorageOptions options)
            : this(storage, options, TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(PostgreSqlStorage storage,  PostgreSqlStorageOptions options, TimeSpan checkInterval)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (options == null) throw new ArgumentNullException(nameof(options));

            _options = options;
            _storage = storage;
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {

            foreach (var table in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                int removedCount;

                do
                {
                    using (var storageConnection = (PostgreSqlConnection) _storage.GetConnection())
                    {
                        using (var transaction = storageConnection.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            removedCount = storageConnection.Connection.Execute(
                                    string.Format(@"
DELETE FROM """ + _options.SchemaName + @""".""{0}"" 
WHERE ""id"" IN (
    SELECT ""id"" 
    FROM """ + _options.SchemaName + @""".""{0}"" 
    WHERE ""expireat"" < NOW() AT TIME ZONE 'UTC' 
    LIMIT {1}
)", table, NumberOfRecordsInSinglePass.ToString(CultureInfo.InvariantCulture)), transaction);

                            transaction.Commit();
                        }
                    }

                    if (removedCount <= 0) continue;
                    Logger.Info($"Removed {removedCount} outdated record(s) from '{table}' table.");

                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                } while (removedCount != 0);
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        public override string ToString()
        {
            return "SQL Records Expiration Manager";
        }
    }
}
