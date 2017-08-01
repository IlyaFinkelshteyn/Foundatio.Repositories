using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Baseline.Reflection;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Marten.Linq;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class SoftDeletesQueryBuilder : IMartenQueryBuilder {
        private static readonly ConcurrentDictionary<Type, SoftDeleteInfo> _softDeleteInfos = new ConcurrentDictionary<Type, SoftDeleteInfo>();

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var softDeleteInfo = _softDeleteInfos.GetOrAdd(typeof(T), t => {
                bool isSupported = typeof(ISupportSoftDeletes).IsAssignableFrom(typeof(T));
                if (!isSupported)
                    return new SoftDeleteInfo { IsSupported = false };

                var field = ctx.Mapping.FieldFor(ReflectionHelper.GetProperty((ISupportSoftDeletes d) => d.IsDeleted));

                return new SoftDeleteInfo {
                    IsSupported = true,
                    SqlField = field.SqlLocator
                };
            });

            if (!softDeleteInfo.IsSupported)
                return Task.CompletedTask;

            var mode = ctx.Source.GetSoftDeleteMode();

            // if we are querying for specific ids then we don't need a deleted filter
            var ids = ctx.Source.GetIds();
            if (ids.Count > 0)
                return Task.CompletedTask;

            if (mode == SoftDeleteQueryMode.All)
                ctx.WhereFragments.Add(new WhereFragment($"{softDeleteInfo.SqlField} is not null"));
            if (mode == SoftDeleteQueryMode.ActiveOnly)
                ctx.WhereFragments.Add(new WhereFragment($"{softDeleteInfo.SqlField} = False"));
            if (mode == SoftDeleteQueryMode.DeletedOnly)
                ctx.WhereFragments.Add(new WhereFragment($"{softDeleteInfo.SqlField} = True"));

            return Task.CompletedTask;
        }

        private class SoftDeleteInfo {
            public bool IsSupported { get; set; }
            public string SqlField { get; set; }
        }
    }
}