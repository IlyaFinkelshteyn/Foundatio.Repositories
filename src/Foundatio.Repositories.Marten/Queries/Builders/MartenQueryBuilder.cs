using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class MartenQueryBuilder : IMartenQueryBuilder {
        private readonly List<IMartenQueryBuilder> _partBuilders = new List<IMartenQueryBuilder>();

        public MartenQueryBuilder(bool registerDefaultBuilders = true) {
            if (registerDefaultBuilders)
                RegisterDefaults();
        }

        public void Register<T>(bool replace = true) where T : IMartenQueryBuilder, new() {
            Register(new T(), replace);
        }

        public void Register(params IMartenQueryBuilder[] builders) {
            foreach (var builder in builders)
                Register(builder);
        }

        public void Register<T>(T builder, bool replace = true) where T : IMartenQueryBuilder {
            if (replace) {
                int existing = _partBuilders.FindIndex(b => b.GetType() == typeof(T));
                if (existing >= 0)
                    _partBuilders.RemoveAt(existing);
            }

            _partBuilders.Add(builder);
        }

        public bool Unregister<T>() where T : IMartenQueryBuilder {
            int existing = _partBuilders.FindIndex(b => b.GetType() == typeof(T));
            if (existing < 0)
                return false;

            _partBuilders.RemoveAt(existing);

            return true;
        }

        public void RegisterDefaults() {
            Register<PagableQueryBuilder>();
            Register<FieldIncludesQueryBuilder>();
            Register(new ParentQueryBuilder(this));
            Register<IdentityQueryBuilder>();
            Register<SoftDeletesQueryBuilder>();
            Register<DateRangeQueryBuilder>();
            Register<FieldConditionsQueryBuilder>();
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            foreach (var builder in _partBuilders)
                await builder.BuildAsync(ctx).AnyContext();
        }

        private static readonly Lazy<MartenQueryBuilder> _default = new Lazy<MartenQueryBuilder>(() => new MartenQueryBuilder());
        public static MartenQueryBuilder Default => _default.Value;
    }
}