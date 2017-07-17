using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class FieldIncludesQueryBuilder : IMartenQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var includes = ctx.Source.GetIncludes();
            if (includes.Count > 0)
                ctx.Search.Source(s => s.Includes(i => i.Fields(includes.ToArray())));

            var excludes = ctx.Source.GetExcludes();
            if (excludes.Count > 0)
                ctx.Search.Source(s => s.Excludes(i => i.Fields(excludes.ToArray())));

            return Task.CompletedTask;
        }
    }
}