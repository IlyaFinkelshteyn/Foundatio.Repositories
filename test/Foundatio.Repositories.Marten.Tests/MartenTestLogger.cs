using System;
using System.Linq;
using System.Text;
using Foundatio.Logging;
using Marten;
using Marten.Services;
using Npgsql;

namespace Foundatio.Repositories.Marten {
    public class MartenTestLogger : IMartenLogger, IMartenSessionLogger {
        private readonly ILogger _logger;

        public MartenTestLogger(ILoggerFactory loggerFactory) {
            _logger = loggerFactory.CreateLogger<MartenTestLogger>();
        }

        public IMartenSessionLogger StartSession(IQuerySession session) {
            return this;
        }

        public void SchemaChange(string sql) {
            var sb = new StringBuilder();
            sb.AppendLine("Executing DDL change:");
            sb.AppendLine(sql);

            _logger.Trace(sb.ToString());
        }

        public void LogSuccess(NpgsqlCommand command) {
            var sb = new StringBuilder();
            sb.AppendLine(command.CommandText);
            foreach (var parameter in command.Parameters) {
                if (parameter is NpgsqlParameter p)
                    sb.AppendLine($"  {p.ParameterName}: {p.Value}");
            }

            _logger.Trace(sb.ToString());
        }

        public void LogFailure(NpgsqlCommand command, Exception ex) {
            var sb = new StringBuilder();
            sb.AppendLine("Postgresql command failed!");
            sb.AppendLine(command.CommandText);
            foreach (var parameter in command.Parameters) {
                if (parameter is NpgsqlParameter p)
                    sb.AppendLine($"  {p.ParameterName}: {p.Value}");
            }

            _logger.Error(ex, sb.ToString());
        }

        public void RecordSavedChanges(IDocumentSession session, IChangeSet commit) {
            var lastCommit = commit;
            _logger.Trace($"Persisted {lastCommit.Updated.Count()} updates, {lastCommit.Inserted.Count()} inserts, and {lastCommit.Deleted.Count()} deletions");
        }
    }
}
