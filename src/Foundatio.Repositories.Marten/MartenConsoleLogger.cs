using System;
using System.Linq;
using Marten;
using Marten.Services;
using Npgsql;

namespace Foundatio.Repositories.Marten {
    public class MartenConsoleLogger : IMartenLogger, IMartenSessionLogger {
        public IMartenSessionLogger StartSession(IQuerySession session) {
            return this;
        }

        public void SchemaChange(string sql) {
            Console.WriteLine("Executing DDL change:");
            Console.WriteLine(sql);
            Console.WriteLine();
        }

        public void LogSuccess(NpgsqlCommand command) {
            Console.WriteLine(command.CommandText);
            foreach (var parameter in command.Parameters) {
                if (parameter is NpgsqlParameter p)
                    Console.WriteLine($"  {p.ParameterName}: {p.Value}");
            }
        }

        public void LogFailure(NpgsqlCommand command, Exception ex) {
            Console.WriteLine("Postgresql command failed!");
            Console.WriteLine(command.CommandText);
            foreach (var parameter in command.Parameters) {
                if (parameter is NpgsqlParameter p)
                    Console.WriteLine($"  {p.ParameterName}: {p.Value}");
            }
            Console.WriteLine(ex);
        }

        public void RecordSavedChanges(IDocumentSession session, IChangeSet commit) {
            var lastCommit = commit;
            Console.WriteLine(
                $"Persisted {lastCommit.Updated.Count()} updates, {lastCommit.Inserted.Count()} inserts, and {lastCommit.Deleted.Count()} deletions");
        }
    }
}
