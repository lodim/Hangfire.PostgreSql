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

namespace Hangfire.PostgreSql.Reboot
{
    public class PostgreSqlStorageOptions
    {
        private TimeSpan _queuePollInterval;

        public PostgreSqlStorageOptions()
        {
            this.QueuePollInterval = TimeSpan.FromSeconds(15);
            this.InvisibilityTimeout = TimeSpan.FromMinutes(30);
            this.SchemaName = "hangfire";
            this.UseNativeDatabaseTransactions = true;
            this.PrepareSchemaIfNecessary = true;
            this.UseConnectionPooling = true;
        }

        public TimeSpan QueuePollInterval
        {
            get { return this._queuePollInterval; }
            set
            {
                var message = $"The QueuePollInterval property value should be positive. Given: {value}.";

                if (value == TimeSpan.Zero)
                    throw new ArgumentException(message, nameof(value));
                if (value != value.Duration())
                    throw new ArgumentException(message, nameof(value));

                this._queuePollInterval = value;
            }
        }

        public TimeSpan InvisibilityTimeout { get; private set; }
        public bool UseNativeDatabaseTransactions { get; set; }
        public bool PrepareSchemaIfNecessary { get; set; }
        public string SchemaName { get; set; }
        public bool UseConnectionPooling { get; set; }
    }
}
