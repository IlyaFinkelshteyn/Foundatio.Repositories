using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories {
    public static class SortQueryExtensions {
        internal const string SortsKey = "@SortsKey";

        public static T Sort<T>(this T query, QueryField field, SortOrder? order = null) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue<T, QueryFieldSort>(SortsKey, new QueryFieldSort { Field = field, Order = order });
        }

        public static T SortDescending<T>(this T query, QueryField field) where T : IRepositoryQuery {
            return query.Sort(field, SortOrder.Descending);
        }

        public static T SortAscending<T>(this T query, QueryField field) where T : IRepositoryQuery {
            return query.Sort(field, SortOrder.Ascending);
        }

        public static IRepositoryQuery<T> Sort<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, SortOrder? order = null) where T : class {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, QueryFieldSort>(SortsKey, new QueryFieldSort { Field = objectPath, Order = order });
        }

        public static IRepositoryQuery<T> SortDescending<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class {
            return query.Sort(objectPath, SortOrder.Descending);
        }

        public static IRepositoryQuery<T> SortAscending<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class {
            return query.Sort(objectPath, SortOrder.Ascending);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadSortQueryExtensions {
        public static ICollection<QueryFieldSort> GetSorts(this IRepositoryQuery query) {
            return query.SafeGetCollection<QueryFieldSort>(SortQueryExtensions.SortsKey);
        }
    }
}
