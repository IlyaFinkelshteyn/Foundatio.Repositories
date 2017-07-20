using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Messaging;
using Foundatio.Repositories.Marten.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Advanced;
using Marten;

namespace Foundatio.Repositories.Marten.Tests {
    public class EmployeeRepository : MartenRepositoryBase<Employee> {
        public EmployeeRepository(IDocumentStore store, ICacheClient cacheClient, ILoggerFactory loggerFactory, IMessagePublisher messagePublisher) : base(store, cacheClient, loggerFactory, messagePublisher) {
            DocumentsChanged.AddHandler((o, args) => {
                DocumentsChangedCount += args.Documents.Count;
                return Task.CompletedTask;
            });

            BeforeQuery.AddHandler((o, args) => {
                QueryCount++;
                return Task.CompletedTask;
            });
        }

        public long DocumentsChangedCount { get; private set; }
        public long QueryCount { get; private set; }

        /// <summary>
        /// This allows us easily test aggregations
        /// </summary>
        public Task<CountResult> GetCountByQueryAsync(RepositoryQueryDescriptor<Employee> query) {
            return this.CountAsync(query);
        }

        public Task<FindResults<Employee>> GetAllByAgeAsync(int age) {
            return this.FindAsync(q => q.Age(age));
        }

        /// <summary>
        /// Exposed only for testing purposes.
        /// </summary>
        public Task<FindResults<Employee>> GetByQueryAsync(RepositoryQueryDescriptor<Employee> query) {
            return this.FindAsync(query);
        }

        public Task<FindResults<Employee>> GetAllByCompanyAsync(string company, CommandOptionsDescriptor<Employee> options = null) {
            var commandOptions = options.Configure();
            if (commandOptions.ShouldUseCache())
                commandOptions.CacheKey(company);

            return this.FindAsync(q => q.Company(company), o => commandOptions);
        }

        public Task<FindResults<Employee>> GetAllByCompaniesWithFieldEqualsAsync(string[] companies) {
            return this.FindAsync(q => q.FieldCondition(c => c.CompanyId, ComparisonOperator.Equals, companies));
        }

        public Task<CountResult> GetCountByCompanyAsync(string company) {
            return this.CountAsync(q => q.Company(company), o => o.CacheKey(company));
        }

        public Task<CountResult> GetNumberOfEmployeesWithMissingCompanyName(string company) {
            return this.CountAsync(q => q.Company(company).FieldCondition(e => e.CompanyName, ComparisonOperator.IsEmpty));
        }

        public Task<CountResult> GetNumberOfEmployeesWithMissingName(string company) {
            return this.CountAsync(q => q.Company(company).FieldCondition(e => e.Name, ComparisonOperator.IsEmpty));
        }

        protected override async Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<Employee>> documents, ICommandOptions options = null) {
            if (!IsCacheEnabled)
                return;

            if (documents != null && documents.Count > 0 && HasIdentity) {
                var keys = documents.Select(d => $"count:{d.Value.CompanyId}").Distinct().ToList();

                if (keys.Count > 0)
                    await Cache.RemoveAllAsync(keys);
            }

            await base.InvalidateCacheAsync(documents, options);
        }
    }
}