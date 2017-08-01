using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class PagableQueryBuilder : IMartenQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            if (ctx.Options.HasPageLimit())
                ctx.AddQueryModifier(q => q.Take(ctx.Options.GetPageLimit()));

            if (ctx.Options.ShouldUseSkip())
                ctx.AddQueryModifier(q => q.Skip(ctx.Options.GetSkip()));

            return Task.CompletedTask;
        }
    }
}
