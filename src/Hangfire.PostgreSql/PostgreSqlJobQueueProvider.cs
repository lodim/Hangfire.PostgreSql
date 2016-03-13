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

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlJobQueueProvider : IPersistentJobQueueProvider
    {
        public PostgreSqlJobQueueProvider(PostgreSqlStorageOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            Options = options;
        }

        public PostgreSqlStorageOptions Options { get; }

        public IPersistentJobQueue GetJobQueue(IDbConnection connection)
        {
            return new PostgreSqlJobQueue(connection, Options);
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(IDbConnection connection)
        {
            return new PostgreSqlJobQueueMonitoringApi(connection, Options);
        }
    }
}