using Foundatio.Logging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public static class ElasticQueryParserConfigurationExtensions {
        public static ElasticQueryParserConfiguration UseMappings<T>(this ElasticQueryParserConfiguration config, IndexTypeBase<T> indexType) where T : class {
            var logger = indexType.Configuration.LoggerFactory.CreateLogger(typeof(ElasticQueryParserConfiguration));
            var descriptor = indexType.BuildMapping(new TypeMappingDescriptor<T>());

            return config
                .UseAliases(indexType.AliasMap)
                .UseMappings<T>(d => descriptor, () => {
                    var response = indexType.Configuration.Client.GetMapping(new GetMappingRequest(indexType.Index.Name, indexType.Name));
                    logger.Trace(() => response.GetRequest());
                    if (!response.IsValid) 
                        logger.Error(response.OriginalException, response.GetErrorMessage());

                    return (ITypeMapping) response.Mapping ?? descriptor;
                });
        }

        public static Field ToNestField(this QueryField queryField) {
            if (queryField.Expression != null)
                return new Field(queryField.Expression);

            if (queryField.Property != null)
                return new Field(queryField.Property);

            return new Field(queryField.Name);
        }

        public static Nest.SortOrder? ToNestOrder(this Repositories.Queries.SortOrder? order) {
            if (!order.HasValue)
                return null;

            return order == Repositories.Queries.SortOrder.Ascending
                ? Nest.SortOrder.Ascending
                : Nest.SortOrder.Descending;
        }
    }
}