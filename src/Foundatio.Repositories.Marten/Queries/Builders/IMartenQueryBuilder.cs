using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;
using Marten;
using Marten.Linq;
using Marten.Schema;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public interface IMartenQueryBuilder {
        Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new();
    }

    public class QueryBuilderContext<T> : IQueryBuilderContext<T> where T : class, new() {
        public QueryBuilderContext(IRepositoryQuery source, ICommandOptions options, DocumentStore store) {
            Source = source;
            Options = options;

            Store = store;
            Mapping = Store.Storage.MappingFor(typeof(T));
            WhereClause = new CompoundWhereFragment("and");
            
            var range = GetDateRange();
            if (range != null) {
                Data.Add(nameof(range.StartDate), range.GetStartDate());
                Data.Add(nameof(range.EndDate), range.GetEndDate());
            }
        }

        public IRepositoryQuery Source { get; }
        public ICommandOptions Options { get; }
        public IWhereFragment WhereClause { get; set; }
        public DocumentStore Store { get; }
        public DocumentMapping Mapping { get; }
        public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();

        private DateRange GetDateRange() {
            foreach (DateRange dateRange in Source.GetDateRanges()) {
                if (dateRange.UseDateRange)
                    return dateRange;
            }

            return null;
        }
    }

    public interface IQueryBuilderContext<T> {
        IRepositoryQuery Source { get; }
        ICommandOptions Options { get; }
        IWhereFragment WhereClause { get; set; }
        DocumentStore Store { get; }
        IDictionary<string, object> Data { get; }
    }

    public static class MartenQueryBuilderExtensions {
        public static async Task ConfigureSearchAsync<T>(this IMartenQueryBuilder builder, IRepositoryQuery query, ICommandOptions options, IDocumentStore store) where T : class, new() {
            if (store == null)
                throw new ArgumentNullException(nameof(store));

            var context = new QueryBuilderContext<T>(query, options, store as DocumentStore);
            await builder.BuildAsync(context).AnyContext();
        }
    }
}