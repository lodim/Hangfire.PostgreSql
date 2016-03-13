using System;
using Hangfire.PostgreSql.Annotations;

namespace Hangfire.PostgreSql
{
    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    internal class FetchedJob
    {
        public int Id { get; set; }
        public int JobId { get; set; }
        public string Queue { get; set; }
        public DateTime? FetchedAt { get; set; }
        public int UpdateCount { get; set; }
    }
}