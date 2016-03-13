// This file is part of Hangfire.PostgreSql.
// Copyright © 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
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
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlConnection : IStorageConnection
    {
        private readonly PersistentJobQueueProviderCollection _queueProviders;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlConnection(
            NpgsqlConnection connection, 
            PersistentJobQueueProviderCollection queueProviders,
            PostgreSqlStorageOptions options,
            bool ownsConnection = true)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (queueProviders == null) throw new ArgumentNullException(nameof(queueProviders));
            if (options == null) throw new ArgumentNullException(nameof(options));

            Connection = connection;
            _queueProviders = queueProviders;
            _options = options;
            OwnsConnection = ownsConnection;
        }

        public bool OwnsConnection { get; }
        public NpgsqlConnection Connection { get; }

        public void Dispose()
        {
            if (OwnsConnection)
            { 
                 Connection.Dispose();
            }
        }

        public IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new PostgreSqlWriteOnlyTransaction(Connection, _options, _queueProviders);
        }

        public IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new PostgreSqlDistributedLock(
                $"HangFire:{resource}",
                timeout,
                Connection,
                _options);
        }

        public IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));

            var providers = queues
                .Select(queue => _queueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException($"Multiple provider instances registered for queues: {string.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
            }

            var persistentQueue = providers[0].GetJobQueue(Connection); 
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters, 
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var createJobSql = @"
INSERT INTO """ + _options.SchemaName + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"", ""expireat"")
VALUES (@invocationData, @arguments, @createdAt, @expireAt) 
RETURNING ""id"";
";

            var invocationData = InvocationData.Serialize(job);

            var jobId = Connection.Query<int>(
                createJobSql,
                new
                {
                    invocationData = JobHelper.ToJson(invocationData),
                    arguments = invocationData.Arguments, createdAt,
                    expireAt = createdAt.Add(expireIn)
                }).Single().ToString(CultureInfo.InvariantCulture);

            if (parameters.Count <= 0) return jobId;
            var parameterArray = new object[parameters.Count];
            var parameterIndex = 0;
            foreach (var parameter in parameters)
            {
                parameterArray[parameterIndex++] = new
                {
                    jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture),
                    name = parameter.Key,
                    value = parameter.Value
                };
            }

            var insertParameterSql = @"
INSERT INTO """ + _options.SchemaName + @""".""jobparameter"" (""jobid"", ""name"", ""value"")
VALUES (@jobId, @name, @value);
";

            Connection.Execute(insertParameterSql, parameterArray);

            return jobId;
        }

        public JobData GetJobData(string id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));

            var sql = 
                @"
SELECT ""invocationdata"" ""invocationData"", ""statename"" ""stateName"", ""arguments"", ""createdat"" ""createdAt"" 
FROM """ + _options.SchemaName + @""".""job"" 
WHERE ""id"" = @id;
";

            var jobData = Connection.Query<SqlJob>(sql, new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture) })
                .SingleOrDefault();

            if (jobData == null) return null;

            // TODO: conversion exception could be thrown.
            var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
            invocationData.Arguments = jobData.Arguments;

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = jobData.StateName,
                CreatedAt = jobData.CreatedAt,
                LoadException = loadException
            };
        }

        public StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            var sql = @"
SELECT s.""name"" ""Name"", s.""reason"" ""Reason"", s.""data"" ""Data""
FROM """ + _options.SchemaName + @""".""state"" s
INNER JOIN """ + _options.SchemaName + @""".""job"" j on j.""stateid"" = s.""id""
WHERE j.""id"" = @jobId;
";

            var sqlState = Connection.Query<SqlState>(sql, new { jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }).SingleOrDefault();
            if (sqlState == null)
            {
                return null;
            }

            return new StateData
            {
                Name = sqlState.Name,
                Reason = sqlState.Reason,
                Data = JobHelper.FromJson<Dictionary<string, string>>(sqlState.Data)
            };
        }

        public void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            var sql = @"
WITH ""inputvalues"" AS (
    SELECT @jobid ""jobid"", @name ""name"", @value ""value""
), ""updatedrows"" AS ( 
    UPDATE """ + _options.SchemaName + @""".""jobparameter"" ""updatetarget""
    SET ""value"" = ""inputvalues"".""value""
    FROM ""inputvalues""
    WHERE ""updatetarget"".""jobid"" = ""inputvalues"".""jobid""
    AND ""updatetarget"".""name"" = ""inputvalues"".""name""
    RETURNING ""updatetarget"".""jobid"", ""updatetarget"".""name""
)
INSERT INTO """ + _options.SchemaName + @""".""jobparameter""(""jobid"", ""name"", ""value"")
SELECT ""jobid"", ""name"", ""value"" 
FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
    SELECT 1 
    FROM ""updatedrows"" 
    WHERE ""updatedrows"".""jobid"" = ""insertvalues"".""jobid"" 
    AND ""updatedrows"".""name"" = ""insertvalues"".""name""
);";

                        Connection.Execute(sql,
                            new { jobId = Convert.ToInt32(id, CultureInfo.InvariantCulture), name, value });
                    }

                    public string GetJobParameter(string id, string name)
                    {
                        if (id == null) throw new ArgumentNullException(nameof(id));
                        if (name == null) throw new ArgumentNullException(nameof(name));

                        return Connection.Query<string>(
                            @"
SELECT ""value"" 
FROM """ + _options.SchemaName + @""".""jobparameter"" 
WHERE ""jobid"" = @id 
AND ""name"" = @name;
",
                            new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture), name })
                            .SingleOrDefault();
                    }

                    public HashSet<string> GetAllItemsFromSet(string key)
                    {
                        if (key == null) throw new ArgumentNullException(nameof(key));

                        var result = Connection.Query<string>(
                            @"
SELECT ""value"" 
FROM """ + _options.SchemaName + @""".""set"" 
WHERE ""key"" = @key;
",
                            new { key });
            
                        return new HashSet<string>(result);
                    }

                    public string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
                    {
                        if (key == null) throw new ArgumentNullException(nameof(key));
                        if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

                        return Connection.Query<string>(
                            @"
SELECT ""value"" 
FROM """ + _options.SchemaName + @""".""set"" 
WHERE ""key"" = @key 
AND ""score"" BETWEEN @from AND @to 
ORDER BY ""score"" LIMIT 1;
",
                            new { key, from = fromScore, to = toScore })
                            .SingleOrDefault();
                    }

                    public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
                    {
                        if (key == null) throw new ArgumentNullException(nameof(key));
                        if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            var sql = @"
WITH ""inputvalues"" AS (
    SELECT @key ""key"", @field ""field"", @value ""value""
), ""updatedrows"" AS ( 
    UPDATE """ + _options.SchemaName + @""".""hash"" ""updatetarget""
    SET ""value"" = ""inputvalues"".""value""
    FROM ""inputvalues""
    WHERE ""updatetarget"".""key"" = ""inputvalues"".""key""
    AND ""updatetarget"".""field"" = ""inputvalues"".""field""
    RETURNING ""updatetarget"".""key"", ""updatetarget"".""field""
)
INSERT INTO """ + _options.SchemaName + @""".""hash""(""key"", ""field"", ""value"")
SELECT ""key"", ""field"", ""value"" FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
    SELECT 1 
    FROM ""updatedrows"" 
    WHERE ""updatedrows"".""key"" = ""insertvalues"".""key"" 
    AND ""updatedrows"".""field"" = ""insertvalues"".""field""
);
";

            using (var transaction = Connection.BeginTransaction(IsolationLevel.Serializable))
            {
                foreach (var keyValuePair in keyValuePairs)
                {
                    Connection.Execute(sql, new {key, field = keyValuePair.Key, value = keyValuePair.Value }, transaction);
                }
                transaction.Commit();
            }
        }

        public Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var result = Connection.Query<SqlHash>(
                @"
SELECT ""field"" ""Field"", ""value"" ""Value"" 
FROM """ + _options.SchemaName + @""".""hash"" 
WHERE ""key"" = @key;
",
                new { key })
                .ToDictionary(x => x.Field, x => x.Value);

            return result.Count != 0 ? result : null;
        }

        public void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow,
            };

            var sql = @"
WITH ""inputvalues"" AS (
    SELECT @id ""id"", @data ""data"", NOW() AT TIME ZONE 'UTC' ""lastheartbeat""
), ""updatedrows"" AS ( 
    UPDATE """ + _options.SchemaName + @""".""server"" ""updatetarget""
    SET ""data"" = ""inputvalues"".""data"", ""lastheartbeat"" = ""inputvalues"".""lastheartbeat""
    FROM ""inputvalues""
    WHERE ""updatetarget"".""id"" = ""inputvalues"".""id""
    RETURNING ""updatetarget"".""id""
)
INSERT INTO """ + _options.SchemaName + @""".""server""(""id"", ""data"", ""lastheartbeat"")
SELECT ""id"", ""data"", ""lastheartbeat"" FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
    SELECT 1 
    FROM ""updatedrows"" 
    WHERE ""updatedrows"".""id"" = ""insertvalues"".""id"" 
);
";

            Connection.Execute(sql,
                new { id = serverId, data = JobHelper.ToJson(data) });
        }

        public void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            Connection.Execute(
                @"
DELETE FROM """ + _options.SchemaName + @""".""server"" 
WHERE ""id"" = @id;
",
                new { id = serverId });
        }

        public void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            Connection.Execute(
                @"
UPDATE """ + _options.SchemaName + @""".""server"" 
SET ""lastheartbeat"" = NOW() AT TIME ZONE 'UTC' 
WHERE ""id"" = @id;
",
                new { id = serverId });
        }

        public int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }

            return Connection.Execute(
                string.Format(@"
DELETE FROM """ + _options.SchemaName + @""".""server"" 
WHERE ""lastheartbeat"" < (NOW() AT TIME ZONE 'UTC' - INTERVAL '{0} MILLISECONDS');
", (long)timeOut.TotalMilliseconds));
        }
    }
}
