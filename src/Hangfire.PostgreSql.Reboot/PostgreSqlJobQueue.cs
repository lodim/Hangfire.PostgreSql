// This file is part of Hangfire.PostgreSql.Reboot
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
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Annotations;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql.Reboot
{
    internal class PostgreSqlJobQueue : IPersistentJobQueue
    {
        private readonly PostgreSqlStorageOptions _options;
        private readonly IDbConnection _connection;

        public PostgreSqlJobQueue(IDbConnection connection, PostgreSqlStorageOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (connection == null) throw new ArgumentNullException(nameof(connection));

            _options = options;
            _connection = connection;
        }


        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (_options.UseNativeDatabaseTransactions)
                return Dequeue_Transaction(queues, cancellationToken);
            else
                return Dequeue_UpdateCount(queues, cancellationToken);
        }


        [NotNull]
        private IFetchedJob Dequeue_Transaction(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            long timeoutSeconds = (long)_options.InvisibilityTimeout.Negate().TotalSeconds;
            FetchedJob fetchedJob;

            string fetchJobSqlTemplate = @"
UPDATE """ + _options.SchemaName + @""".""jobqueue"" 
SET ""fetchedat"" = NOW() AT TIME ZONE 'UTC'
WHERE ""id"" IN (
    SELECT ""id"" 
    FROM """ + _options.SchemaName + @""".""jobqueue"" 
    WHERE ""queue"" = ANY (@queues)
    AND ""fetchedat"" {0} 
    ORDER BY ""fetchedat"", ""jobid""
    LIMIT 1
)
RETURNING ""id"" AS ""Id"", ""jobid"" AS ""JobId"", ""queue"" AS ""Queue"", ""fetchedat"" AS ""FetchedAt"";
";

            var fetchConditions = new[] { "IS NULL", $"< NOW() AT TIME ZONE 'UTC' + INTERVAL '{timeoutSeconds} SECONDS'"};
            var currentQueryIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                string fetchJobSql = string.Format(fetchJobSqlTemplate, fetchConditions[currentQueryIndex]);

                Utilities.TryExecute(() =>
                {
                    using (var trx = _connection.BeginTransaction(IsolationLevel.RepeatableRead))
                    {
                        var jobToFetch = _connection.Query<FetchedJob>(
                            fetchJobSql,
                            new { queues = queues.ToList() }, trx)
                            .SingleOrDefault();

                        trx.Commit();

                        return jobToFetch;
                    }
                },
                    out fetchedJob,
                    ex => ex is NpgsqlException && ((NpgsqlException) ex).Code == "40001");

                if (fetchedJob == null)
                {
                    if (currentQueryIndex == fetchConditions.Length - 1)
                    {
                        cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }

                currentQueryIndex = (currentQueryIndex + 1) % fetchConditions.Length;
            } while (fetchedJob == null);

            return new PostgreSqlFetchedJob(
                _connection,
                _options,
                fetchedJob.Id,
                fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                fetchedJob.Queue);
        }


        [NotNull]
        private IFetchedJob Dequeue_UpdateCount(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));


            long timeoutSeconds = (long)_options.InvisibilityTimeout.Negate().TotalSeconds;
            FetchedJob markJobAsFetched = null;


            string jobToFetchSqlTemplate = @"
SELECT ""id"" AS ""Id"", ""jobid"" AS ""JobId"", ""queue"" AS ""Queue"", ""fetchedat"" AS ""FetchedAt"", ""updatecount"" AS ""UpdateCount""
FROM """ + _options.SchemaName + @""".""jobqueue"" 
WHERE ""queue"" = ANY (@queues)
AND ""fetchedat"" {0} 
ORDER BY ""fetchedat"", ""jobid"" 
LIMIT 1;
";

            string markJobAsFetchedSql = @"
UPDATE """ + _options.SchemaName + @""".""jobqueue"" 
SET ""fetchedat"" = NOW() AT TIME ZONE 'UTC', 
    ""updatecount"" = (""updatecount"" + 1) % 2000000000
WHERE ""id"" = @Id 
AND ""updatecount"" = @UpdateCount
RETURNING ""id"" AS ""Id"", ""jobid"" AS ""JobId"", ""queue"" AS ""Queue"", ""fetchedat"" AS ""FetchedAt"";
";

            var fetchConditions = new[] { "IS NULL", $"< NOW() AT TIME ZONE 'UTC' + INTERVAL '{timeoutSeconds} SECONDS'" };
            var currentQueryIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                string jobToFetchJobSql = string.Format(jobToFetchSqlTemplate, fetchConditions[currentQueryIndex]);

                FetchedJob jobToFetch = _connection.Query<FetchedJob>(
                    jobToFetchJobSql,
                    new { queues = queues.ToList() })
                    .SingleOrDefault();

                if (jobToFetch == null)
                {
                    if (currentQueryIndex == fetchConditions.Length - 1)
                    {
                        cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }
                else
                {
                    markJobAsFetched = _connection.Query<FetchedJob>(
                        markJobAsFetchedSql,
                        jobToFetch)
                        .SingleOrDefault();
                }



                currentQueryIndex = (currentQueryIndex + 1) % fetchConditions.Length;
            } while (markJobAsFetched == null);

            return new PostgreSqlFetchedJob(
                _connection,
                _options,
                markJobAsFetched.Id,
                markJobAsFetched.JobId.ToString(CultureInfo.InvariantCulture),
                markJobAsFetched.Queue);
        }

        public void Enqueue(string queue, string jobId)
        {
            string enqueueJobSql = @"
INSERT INTO """ + _options.SchemaName + @""".""jobqueue"" (""jobid"", ""queue"") 
VALUES (@jobId, @queue);
";

            _connection.Execute(enqueueJobSql, new { jobId = Convert.ToInt32(jobId,CultureInfo.InvariantCulture), queue });
        }
    }
}
