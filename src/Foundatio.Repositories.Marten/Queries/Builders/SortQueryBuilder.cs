using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class SortQueryBuilder : IMartenQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var sortFields = ctx.Source.GetSorts();
            if (sortFields.Count <= 0)
                return Task.CompletedTask;

            //var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            //foreach (var sort in sortableQuery.SortFields.Where(s => CanSortByField(opt?.AllowedSortFields, s.Field)))
            //    ctx.Search.Sort(s => s.Field(sort.Field, sort.Order == Foundatio.Repositories.Models.SortOrder.Ascending ? SortOrder.Ascending : SortOrder.Descending));
            ctx.Search.Sort(sortFields.Select(f => new SortField { Field = f.Field.ToNestField(), Order = f.Order.ToNestOrder() }));

            return Task.CompletedTask;
        }

        protected bool CanSortByField(ISet<string> allowedFields, string field) {
            // allow all fields if an allowed list isn't specified
            if (allowedFields == null || allowedFields.Count == 0)
                return true;

            return allowedFields.Contains(field, StringComparer.OrdinalIgnoreCase);
        }
    }
}