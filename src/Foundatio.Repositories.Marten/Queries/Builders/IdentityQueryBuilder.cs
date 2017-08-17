using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Queries;
using Marten.Linq;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class IdentityQueryBuilder : IMartenQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var ids = ctx.Source.GetIds().ToArray();
            var idField = ctx.Mapping.FieldFor(ctx.Mapping.IdMember);
            
            if (ids.Length == 1)
                ctx.WhereFragments.Add(new WhereFragment($"{idField.SqlLocator} = ?", ids.First()));
            else if (ids.Length > 1)
                ctx.WhereFragments.Add(new WhereFragment($"{idField.SqlLocator} = ANY(?)", ids));

            var excludesIds = ctx.Source.GetExcludedIds();
            if (excludesIds.Count == 1)
                ctx.WhereFragments.Add(new WhereFragment($"{idField.SqlLocator} != ?", ids.First()));
            else if (excludesIds.Count > 1)
                ctx.WhereFragments.Add(new WhereFragment($"{idField.SqlLocator} != ANY(?)", ids));

            return Task.CompletedTask;
        }
    }
}