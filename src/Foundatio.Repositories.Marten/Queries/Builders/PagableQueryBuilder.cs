using System.Threading.Tasks;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class PagableQueryBuilder : IMartenQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            // add 1 to limit if not snapshot paging so we can know if we have more results
            //if (ctx.Options.HasPageLimit())
            //    ctx.Search.Size(ctx.Options.GetLimit() + (ctx.Options.ShouldUseSnapshotPaging() == false ? 1 : 0));
                
            //if (ctx.Options.ShouldUseSkip())
            //    ctx.Search.Skip(ctx.Options.GetSkip());

            return Task.CompletedTask;
        }
    }
}
