Hangfire.PostgreSql.Reboot
===================
This is an plugin to the Hangfire to enable PostgreSQL as a storage system.
Read about hangfire here: https://github.com/HangfireIO/Hangfire#hangfire-
and here: http://hangfire.io/

Apart from other similar plugins, this one does not use connection pooling.

Instructions
------------

Hangfire.PostgreSql.Reboot is available as a NuGet package. You can install it using the NuGet Package Console window:

```
PM> Install-Package Hangfire.PostgreSql.Reboot
```

Then, configure Hangfire by using the following code:

```csharp
private static void ConfigureHangfire(IAppBuilder app)
{
	GlobalConfiguration.Configuration.UseStorage(new PostgreSqlStorage("ConnectionString"));
	//GlobalConfiguration.Configuration.UseNLogLogProvider();
	app.UseHangfireServer(new BackgroundJobServerOptions
	{
		Queues = new[] { "critical", "default" }
	});
}
```

Furthermore, one may be interested in using the Hangfire.Dashboard as well:

```
PM> Install-Package Hangfire.Dashboard.Authorization
```


Related Projects
-----------------

* [Hangfire.Core](https://github.com/HangfireIO/Hangfire)
* [Hangfire.Dashboard.Authorization](https://github.com/HangfireIO/Hangfire.Dashboard.Authorization)

License
--------

Copyright © 2016 Mihai Bogdan Eugen.

Hangfire.PostgreSql is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as 
published by the Free Software Foundation, either version 3 
of the License, or any later version.

Hangfire.PostgreSql  is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public 
License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.

This work is based on the works of Frank Hommers and Sergey Odinokov, the author of Hangfire. <http://hangfire.io/>
  
   Special thanks goes to them.
