using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Baseline;
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
        private readonly IList<Func<IQueryable<T>, IQueryable<T>>> _queryModifiers = new List<Func<IQueryable<T>, IQueryable<T>>>();

        public QueryBuilderContext(IRepositoryQuery source, ICommandOptions options, DocumentStore store) {
            Source = source;
            Options = options;

            Store = store;
            Mapping = Store.Storage.MappingFor(typeof(T));
            WhereFragments = new List<IWhereFragment>();
            
            var range = GetDateRange();
            if (range != null) {
                Data.Add(nameof(range.StartDate), range.GetStartDate());
                Data.Add(nameof(range.EndDate), range.GetEndDate());
            }
        }

        public IRepositoryQuery Source { get; }
        public ICommandOptions Options { get; }
        public ICollection<IWhereFragment> WhereFragments { get; }
        public DocumentStore Store { get; }
        public DocumentMapping Mapping { get; }
        public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();

        public IQueryable<T> ConfigureQuery(IQueryable<T> query) {
            return _queryModifiers.Aggregate(query, (current, modifier) => modifier(current));
        }

        public void AddQueryModifier(Func<IQueryable<T>, IQueryable<T>> modifier) {
            _queryModifiers.Add(modifier);
        }

        public void QueryAnd<TModel>(Expression<Func<TModel, bool>> expression) {
            Expression converted = Expression.Convert(expression.Body, typeof(object));
            AddQueryModifier(q => q.Where(Expression.Lambda<Func<T, bool>>(converted, expression.Parameters)));
        }

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
        ICollection<IWhereFragment> WhereFragments { get; }
        DocumentStore Store { get; }
        IDictionary<string, object> Data { get; }
    }

    public static class MartenQueryBuilderExtensions {
        public static IMartenQueryable<T> Query<T>(this IQuerySession session, IRepositoryQuery query, ICommandOptions options) where T : class, new() {
            var context = new QueryBuilderContext<T>(query, options, session.DocumentStore as DocumentStore);
            MartenQueryBuilder.Default.BuildAsync(context).AnyContext().GetAwaiter().GetResult();
            var result = session.Query<T>();
            var q = context.ConfigureQuery(result);
            return q.As<IMartenQueryable<T>>();
        }

        public static async Task<QueryBuilderContext<T>> BuildAsync<T>(this IMartenQueryBuilder builder, IRepositoryQuery query, ICommandOptions options, IDocumentStore store) where T : class, new() {
            if (store == null)
                throw new ArgumentNullException(nameof(store));

            var context = new QueryBuilderContext<T>(query, options, store as DocumentStore);
            await builder.BuildAsync(context).AnyContext();

            return context;
        }
    }
}