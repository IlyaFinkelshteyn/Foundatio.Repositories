using System;
using System.Linq;
using System.Linq.Expressions;
using Marten;
using Marten.Linq;
using Marten.Linq.Parsing;
using Marten.Schema;
using Marten.Services;
using Npgsql;

namespace Test {
    class Program {
        static void Main() {
            var store = DocumentStore.For(st => {
                st.Connection("host=localhost;database=marten;username=postgres;password=banana");
                st.Logger(new MyConsoleMartenLogger());
                st.Linq.MethodCallParsers.Add(new MatchesWhereFragmentParser());

                st.Schema.For<Organization>().SoftDeleted();
                st.Schema.For<User>()
                    .UseOptimisticConcurrency(true)
                    .Duplicate(u => u.FirstName)
                    .Duplicate(u => u.LastName)
                    .Duplicate(u => u.Counter)
                    .ForeignKey<Organization>(u => u.OrganizationId)
                    .SoftDeleted();
            });

            store.Advanced.Clean.CompletelyRemoveAll();
            store.Schema.ApplyAllConfiguredChangesToDatabase();

            Guid userId;
            Guid orgId = Guid.NewGuid();
            using (var session = store.LightweightSession()) {
                var org = new Organization { Id = orgId, Name = "Star Wars" };
                var user = new User { OrganizationId = orgId, FirstName = "Han", LastName = "Solo", Counter = 0 };
                session.Store(user);
                // will save the org before the user
                session.Store(org);

                session.SaveChanges();
                userId = user.Id;

                var user2 = new User { OrganizationId = orgId, FirstName = "Han", LastName = "Solo", Counter = 3 };
                session.Store(user2);
                session.SaveChanges();
            }

            using (var session = store.LightweightSession()) {
                session.Patch<User>(userId).Increment(x => x.Counter, 5);
                session.Patch<User>(userId).Increment(x => x.Counter, 2);
                session.Patch<User>(userId).Set(x => x.UserName, "New UserName");

                session.SaveChanges();
            }

            using (var session = store.DirtyTrackedSession()) {
                Organization org = null;
                var user = session
                    .Query<User>()
                    .Include<Organization>(u => u.OrganizationId, o => org = o)
                    .FirstOrDefault(x => x.MatchesWhereFragment("first_name = ? and last_name = ?", "Han", "Solo"));

                user = session
                    .Query<User>()
                    .Where(x => x.MatchesWhereFragment("first_name = ? and last_name = ?", "Han", "Solo"))
                    .Skip(1).Take(1)
                    .FirstOrDefault();

                user.FirstName = "Eric";
                org.Name = "New Name";

                session.SaveChanges();
            }

            using (var session = store.QuerySession()) {
                var b = session.CreateBatchQuery();
                var org = b.Load<Organization>(orgId);
                var user = b.Load<User>(userId);
                b.ExecuteSynchronously();
            }
        }
    }

    public class Organization {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }

    public class User {
        public Guid Id { get; set; }
        public Guid OrganizationId { get; set; }
        public int Counter { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public bool Internal { get; set; }
        public string UserName { get; set; }
    }

    public class MyConsoleMartenLogger : IMartenLogger, IMartenSessionLogger {
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
            foreach (object parameter in command.Parameters) {
                if (parameter is NpgsqlParameter p)
                    Console.WriteLine($"  {p.ParameterName}: {p.Value}");
            }
        }

        public void LogFailure(NpgsqlCommand command, Exception ex) {
            Console.WriteLine("Postgresql command failed!");
            Console.WriteLine(command.CommandText);
            foreach (object parameter in command.Parameters) {
                if (parameter is NpgsqlParameter p)
                    Console.WriteLine($"  {p.ParameterName}: {p.Value}");
            }
            Console.WriteLine(ex);
        }

        public void RecordSavedChanges(IDocumentSession session, IChangeSet commit) {
            var lastCommit = commit;
            Console.WriteLine($"Persisted {lastCommit.Updated.Count()} updates, {lastCommit.Inserted.Count()} inserts, and {lastCommit.Deleted.Count()} deletions");
        }
    }

    public class MatchesWhereFragmentParser : IMethodCallParser {
        public bool Matches(MethodCallExpression expression) {
            return expression.Method.Name == nameof(MatchesWhereFragmentParserExtensions.MatchesWhereFragment);
        }

        public IWhereFragment Parse(IQueryableDocument mapping, ISerializer serializer, MethodCallExpression expression) {
            if (expression.Method.GetParameters().Length == 3)
                return new WhereFragment(expression.Arguments[1].Value() as string, expression.Arguments[2].Value() as object[]);
            
            return expression.Arguments[1].Value() as IWhereFragment;
        }
    }

    public static class MatchesWhereFragmentParserExtensions {
        public static bool MatchesWhereFragment(this object target, IWhereFragment whereFragment) {
            return true;
        }

        public static bool MatchesWhereFragment(this object target, string sql, params object[] parameters) {
            return true;
        }
    }
}

