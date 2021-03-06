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
using Dapper;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Reboot
{
    internal class PostgreSqlFetchedJob : IFetchedJob
    {
        private readonly IDbConnection _connection;
        private readonly PostgreSqlStorageOptions _options;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public PostgreSqlFetchedJob(
            IDbConnection connection, 
            PostgreSqlStorageOptions options,
            int id, 
            string jobId, 
            string queue)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (options == null) throw new ArgumentNullException(nameof(options));

            _connection = connection;
            _options = options;

            Id = id;
            JobId = jobId;
            Queue = queue;
        }

        public int Id { get; }
        public string JobId { get; }
        public string Queue { get; private set; }

        public void RemoveFromQueue()
        {
            _connection.Execute(
                @"
DELETE FROM """ + _options.SchemaName + @""".""jobqueue"" 
WHERE ""id"" = @id;
",
                new { id = Id });

            _removedFromQueue = true;
        }

        public void Requeue()
        {
            _connection.Execute(
                @"
UPDATE """ + _options.SchemaName + @""".""jobqueue"" 
SET ""fetchedat"" = NULL 
WHERE ""id"" = @id;
",
                new { id = Id });

            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }
    }
}
