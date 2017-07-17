using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class FieldIncludesQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var includes = ctx.Source.GetIncludes();
            if (includes.Count > 0)
                ctx.Search.Source(s => s.Includes(i => i.Fields(includes.Select(f => f.ToNestField()).ToArray())));

            var excludes = ctx.Source.GetExcludes();
            if (excludes.Count > 0)
                ctx.Search.Source(s => s.Excludes(i => i.Fields(excludes.Select(f => f.ToNestField()).ToArray())));

            return Task.CompletedTask;
        }
    }
}