using System.Threading.Tasks;
using Foundatio.Repositories.Queries;
using Marten.Linq;
using Marten.Schema;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class SoftDeletesQueryBuilder : IMartenQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            if (ctx.Mapping.DeleteStyle != DeleteStyle.SoftDelete)
                return Task.CompletedTask;

            var mode = ctx.Source.GetSoftDeleteMode();

            // if we are querying for specific ids then we don't need a deleted filter
            var ids = ctx.Source.GetIds();
            if (ids.Count > 0)
                return Task.CompletedTask;

            if (mode == SoftDeleteQueryMode.All)
                ctx.WhereFragments.Add(new WhereFragment($"d.{DocumentMapping.DeletedColumn} is not null"));
            if (mode == SoftDeleteQueryMode.ActiveOnly)
                ctx.WhereFragments.Add(new WhereFragment($"d.{DocumentMapping.DeletedColumn} = False"));
            if (mode == SoftDeleteQueryMode.DeletedOnly)
                ctx.WhereFragments.Add(new WhereFragment($"d.{DocumentMapping.DeletedColumn} = True"));

            return Task.CompletedTask;
        }
    }
}