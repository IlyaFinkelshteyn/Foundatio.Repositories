using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class DateRangeQueryBuilder : IMartenQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var dateRanges = ctx.Source.GetDateRanges();
            if (dateRanges.Count <= 0)
                return Task.CompletedTask;

            var elasticQueryOptions = ctx.Options.GetElasticTypeSettings();
            foreach (var dateRange in dateRanges.Where(dr => dr.UseDateRange)) {
                string fieldName = dateRange.Field?.Name;
                if (elasticQueryOptions?.IndexType != null && !String.IsNullOrEmpty(fieldName))
                    fieldName = elasticQueryOptions.IndexType.GetFieldName(fieldName);

                ctx.Filter &= new DateRangeQuery {
                    Field = fieldName ?? dateRange.Field,
                    GreaterThanOrEqualTo = dateRange.GetStartDate(),
                    LessThanOrEqualTo = dateRange.GetEndDate()
                };
            }

            return Task.CompletedTask;
        }
    }
}