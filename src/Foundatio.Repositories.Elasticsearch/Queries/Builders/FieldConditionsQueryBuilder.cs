using System.Collections.Generic;
using System.Threading.Tasks;
using Nest;
using System.Collections;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class FieldConditionsQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var fieldConditions = ctx.Source.GetFieldConditions();
            if (fieldConditions == null || fieldConditions.Count <= 0)
                return Task.CompletedTask;

            foreach (var fieldValue in fieldConditions) {
                QueryBase query;
                switch (fieldValue.Operator) {
                    case ComparisonOperator.Equals:
                        if (fieldValue.Value is IEnumerable && !(fieldValue.Value is string))
                            query = new TermsQuery { Field = fieldValue.Field.ToNestField(), Terms = (IEnumerable<object>)fieldValue.Value };
                        else
                            query = new TermQuery { Field = fieldValue.Field.ToNestField(), Value = fieldValue.Value };
                        ctx.Filter &= query;

                        break;
                    case ComparisonOperator.NotEquals:
                        if (fieldValue.Value is IEnumerable && !(fieldValue.Value is string))
                            query = new TermsQuery { Field = fieldValue.Field.ToNestField(), Terms = (IEnumerable<object>)fieldValue.Value };
                        else
                            query = new TermQuery { Field = fieldValue.Field.ToNestField(), Value = fieldValue.Value };

                        ctx.Filter &= new BoolQuery { MustNot = new QueryContainer[] { query } };
                        break;
                    case ComparisonOperator.IsEmpty:
                        ctx.Filter &= new BoolQuery { MustNot = new QueryContainer[] { new ExistsQuery { Field = fieldValue.Field.ToNestField() } } };
                        break;
                    case ComparisonOperator.HasValue:
                        ctx.Filter &= new ExistsQuery { Field = fieldValue.Field.ToNestField() };
                        break;
                }
            }

            return Task.CompletedTask;
        }
    }
}