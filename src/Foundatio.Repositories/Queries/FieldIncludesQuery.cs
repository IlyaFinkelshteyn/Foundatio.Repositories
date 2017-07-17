using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories {
    public static class FieldIncludesQueryExtensions {
        internal const string IncludesKey = "@Includes";
        public static T Include<T>(this T query, QueryField field) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(IncludesKey, field);
        }

        public static T Include<T>(this T query, IEnumerable<QueryField> fields) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(IncludesKey, fields);
        }

        public static IRepositoryQuery Include<T>(this IRepositoryQuery query, Expression<Func<T, object>> objectPath) {
            return query.AddCollectionOptionValue<IRepositoryQuery, QueryField>(IncludesKey, objectPath);
        }

        public static IRepositoryQuery<T> Include<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T: class {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, QueryField>(IncludesKey, objectPath);
        }

        public static IRepositoryQuery<T> Include<T>(this IRepositoryQuery<T> query, params Expression<Func<T, object>>[] objectPaths) where T : class {
            foreach (var objectPath in objectPaths)
                query.Include(objectPath);

            return query;
        }

        internal const string ExcludesKey = "@Excludes";
        public static T Exclude<T>(this T query, QueryField field) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(ExcludesKey, field);
        }

        public static T Exclude<T>(this T query, IEnumerable<QueryField> fields) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(ExcludesKey, fields);
        }

        public static IRepositoryQuery<T> Exclude<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, QueryField>(ExcludesKey, objectPath);
        }

        public static IRepositoryQuery<T> Exclude<T>(this IRepositoryQuery<T> query, params Expression<Func<T, object>>[] objectPaths) where T : class {
            foreach (var objectPath in objectPaths)
                query.Exclude(objectPath);

            return query;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadFieldIncludesQueryExtensions {
        public static ICollection<QueryField> GetIncludes(this IRepositoryQuery options) {
            return options.SafeGetCollection<QueryField>(FieldIncludesQueryExtensions.IncludesKey);
        }

        public static ICollection<QueryField> GetExcludes(this IRepositoryQuery options) {
            return options.SafeGetCollection<QueryField>(FieldIncludesQueryExtensions.ExcludesKey);
        }
    }
}
