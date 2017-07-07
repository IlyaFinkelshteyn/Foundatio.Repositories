using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Marten.Queries.Builders {
    public class LinqQueryBuilder : ILinqQueryBuilder {
        private readonly List<ILinqQueryBuilder> _partBuilders = new List<ILinqQueryBuilder>();

        public LinqQueryBuilder(bool registerDefaultBuilders = true) {
            if (registerDefaultBuilders)
                RegisterDefaults();
        }

        public void Register<T>(bool replace = true) where T : ILinqQueryBuilder, new() {
            Register(new T(), replace);
        }

        public void Register(params ILinqQueryBuilder[] builders) {
            foreach (var builder in builders)
                Register(builder);
        }

        public void Register<T>(T builder, bool replace = true) where T : ILinqQueryBuilder {
            if (replace) {
                int existing = _partBuilders.FindIndex(b => b.GetType() == typeof(T));
                if (existing >= 0)
                    _partBuilders.RemoveAt(existing);
            }

            _partBuilders.Add(builder);
        }

        public bool Unregister<T>() where T : ILinqQueryBuilder {
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
            Register(new ChildQueryBuilder(this));
            Register<IdentityQueryBuilder>();
            Register<SoftDeletesQueryBuilder>();
            Register<DateRangeQueryBuilder>();
            Register<FieldConditionsQueryBuilder>();
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            foreach (var builder in _partBuilders)
                await builder.BuildAsync(ctx).AnyContext();
        }

        private static readonly Lazy<LinqQueryBuilder> _default = new Lazy<LinqQueryBuilder>(() => new LinqQueryBuilder());
        public static LinqQueryBuilder Default => _default.Value;
    }
}