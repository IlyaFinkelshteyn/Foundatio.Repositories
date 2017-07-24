using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Marten.Tests.Repositories.Models;
using Foundatio.Repositories.Marten.Queries.Builders;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class CompanyQueryExtensions {
        internal const string CompaniesKey = "@Companies";

        public static T Company<T>(this T query, string companyId) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(CompaniesKey, companyId);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadCompanyQueryExtensions {
        public static ICollection<string> GetCompanies(this IRepositoryQuery query) {
            return query.SafeGetCollection<string>(CompanyQueryExtensions.CompaniesKey);
        }
    }
}

namespace Foundatio.Repositories.Marten.Tests.Repositories.Queries {
    public class CompanyQueryBuilder : IMartenQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var companyIds = ctx.Source.GetCompanies();
            if (companyIds.Count <= 0)
                return Task.CompletedTask;

            if (companyIds.Count == 1)
                ctx.AddFilter<Employee>(e => e.CompanyId == companyIds.Single());
            else
                ctx.AddFilter<Employee>(e => companyIds.Contains(e.CompanyId));

            return Task.CompletedTask;
        }
    }
}