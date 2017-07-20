using System.Collections.Generic;
using System.Threading.Tasks;
using System.Collections;
using Foundatio.Repositories.Marten.Extensions;
using Foundatio.Repositories.Options;
using Marten.Linq;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class FieldConditionsQueryBuilder : IMartenQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var fieldConditions = ctx.Source.GetFieldConditions();
            if (fieldConditions == null || fieldConditions.Count <= 0)
                return Task.CompletedTask;
            
            foreach (var fieldValue in fieldConditions) {
                var field = ctx.Mapping.FieldFor(fieldValue.Field);
                
                switch (fieldValue.Operator) {
                    case ComparisonOperator.Equals:
                        if (fieldValue.Value is IEnumerable && !(fieldValue.Value is string))
                            ctx.WhereFragments.Add(new WhereFragment($"{field.SqlLocator} = ANY(?)", (IEnumerable<object>)fieldValue.Value));
                        else
                            ctx.WhereFragments.Add(new WhereFragment($"{field.SqlLocator} = ?", fieldValue.Value));

                        break;
                    case ComparisonOperator.NotEquals:
                        if (fieldValue.Value is IEnumerable && !(fieldValue.Value is string))
                            ctx.WhereFragments.Add(new WhereFragment($"{field.SqlLocator} != ANY(?)", (IEnumerable<object>)fieldValue.Value));
                        else
                            ctx.WhereFragments.Add(new WhereFragment($"{field.SqlLocator} != ?", fieldValue.Value));

                        break;
                    case ComparisonOperator.IsEmpty:
                        ctx.WhereFragments.Add(new WhereFragment($"{field.SqlLocator} is null"));
                        break;
                    case ComparisonOperator.HasValue:
                        ctx.WhereFragments.Add(new WhereFragment($"{field.SqlLocator} is not null"));
                        break;
                }
            }

            return Task.CompletedTask;
        }
    }
}