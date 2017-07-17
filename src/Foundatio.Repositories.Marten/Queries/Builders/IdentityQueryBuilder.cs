using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Queries;
using Marten.Linq;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class IdentityQueryBuilder : IMartenQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var ids = ctx.Source.GetIds();
            var idField = ctx.Mapping.FieldFor(ctx.Mapping.IdMember);
            
            if (ids.Count > 0)
                ctx.WhereClause.Append(new WhereFragment($"{idField.SqlLocator} = ANY(?)", ids));

            var excludesIds = ctx.Source.GetExcludedIds();
            if (excludesIds.Count > 0)
                ctx.WhereClause.Append(new WhereFragment($"{idField.SqlLocator} != ANY(?)", ids));

            return Task.CompletedTask;
        }
    }
}