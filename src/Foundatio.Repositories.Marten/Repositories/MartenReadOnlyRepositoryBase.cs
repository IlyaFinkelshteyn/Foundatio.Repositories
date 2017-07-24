using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using AutoMapper.QueryableExtensions;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Advanced;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Marten.Queries.Builders;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Foundatio.Utility;
using Marten;
using Marten.Linq;
using Marten.Schema;

namespace Foundatio.Repositories.Marten {
    public abstract class MartenReadOnlyRepositoryBase<T> : IAdvancedReadOnlyRepository<T> where T : class, new() {
        protected static readonly bool HasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
        protected static readonly bool HasDates = typeof(IHaveDates).IsAssignableFrom(typeof(T));
        protected static readonly bool HasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
        protected static readonly bool SupportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(typeof(T));
        protected static readonly bool HasVersion = typeof(IVersioned).IsAssignableFrom(typeof(T));
        protected static readonly string EntityTypeName = typeof(T).Name;
        protected static readonly IReadOnlyCollection<T> EmptyList = new List<T>(0).AsReadOnly();
        protected readonly string _idField = null;
        protected readonly DocumentStore _store;
        protected readonly DocumentMapping _mapping;
        protected readonly ILogger _logger;
        protected readonly IMapper _mapper;

        private ScopedCacheClient _scopedCacheClient;

        protected MartenReadOnlyRepositoryBase(IDocumentStore store, ICacheClient cacheClient, ILoggerFactory loggerFactory) {
            _store = store as DocumentStore;
            _mapping = _store.Storage.MappingFor(typeof(T));
            if (HasIdentity)
                _idField = _mapping.FieldFor(_mapping.IdMember).SqlLocator;

            SetCache(cacheClient);
            _logger = loggerFactory.CreateLogger(GetType());

            var config = new MapperConfiguration(cfg => { cfg.CreateMissingTypeMaps = true; });
            _mapper = config.CreateMapper();
        }

        protected ICollection<QueryField> DefaultExcludes { get; } = new List<QueryField>();

        public Task<FindResults<T>> FindAsync(IRepositoryQuery query, ICommandOptions options = null) {
            return FindAsAsync<T>(query, options);
        }

        public async Task<FindResults<TResult>> FindAsAsync<TResult>(IRepositoryQuery query, ICommandOptions options = null) where TResult : class, new() {
            if (query == null)
                query = new RepositoryQuery();

            bool allowCaching = IsCacheEnabled;

            options = ConfigureOptions(options);
            await OnBeforeQueryAsync(query, options, typeof(TResult)).AnyContext();

            async Task<FindResults<TResult>> GetNextPageFunc(FindResults<TResult> r) {
                var previousResults = r;
                if (previousResults == null)
                    throw new ArgumentException(nameof(r));

                if (options == null)
                    return new FindResults<TResult>();

                options?.PageNumber(!options.HasPageNumber() ? 2 : options.GetPage() + 1);

                return await FindAsAsync<TResult>(query, options).AnyContext();
            }

            string cacheSuffix = options?.HasPageLimit() == true ? String.Concat(options.GetPage().ToString(), ":", options.GetLimit().ToString()) : null;

            FindResults<TResult> result;
            if (allowCaching) {
                result = await GetCachedQueryResultAsync<FindResults<TResult>>(options, cacheSuffix: cacheSuffix).AnyContext();
                if (result != null) {
                    ((IGetNextPage<TResult>)result).GetNextPageFunc = async r => await GetNextPageFunc(r).AnyContext();
                    return result;
                }
            }

            using (var session = _store.QuerySession()) {
                QueryStatistics stats;
                var response = session.Query<T>(query, options).Stats(out stats).ToList();

                if (options.HasPageLimit()) {
                    result = ToFindResults(response.Select(r => _mapper.Map<T, TResult>(r)).ToList(), stats.TotalResults);
                    result.HasMore = response.Count > options.GetLimit();
                    ((IGetNextPage<TResult>) result).GetNextPageFunc = GetNextPageFunc;
                } else {
                    result = ToFindResults(response.Select(r => _mapper.Map<T, TResult>(r)).ToList(), stats.TotalResults);
                }

                result.Page = options.GetPage();

                if (!allowCaching)
                    return result;

                var nextPageFunc = ((IGetNextPage<TResult>)result).GetNextPageFunc;
                ((IGetNextPage<TResult>)result).GetNextPageFunc = null;
                await SetCachedQueryResultAsync(options, result, cacheSuffix: cacheSuffix).AnyContext();
                ((IGetNextPage<TResult>)result).GetNextPageFunc = nextPageFunc;

                return result;
            }
        }

        protected FindResults<TResult> ToFindResults<TResult>(IReadOnlyList<TResult> results, long total) where TResult : class {
            return new FindResults<TResult>(results.Select(ToFindHit), total);
        }

        protected FindHit<TResult> ToFindHit<TResult>(TResult doc) where TResult : class {
            return new FindHit<TResult>(null, doc, 0);
        }

        public async Task<FindHit<T>> FindOneAsync(IRepositoryQuery query, ICommandOptions options = null) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var result = IsCacheEnabled ? await GetCachedQueryResultAsync<FindHit<T>>(options).AnyContext() : null;
            if (result != null)
                return result;

            options = ConfigureOptions(options);
            await OnBeforeQueryAsync(query, options, typeof(T)).AnyContext();

            using (var session = _store.QuerySession()) {
                var response = await session.Query<T>(query, options).FirstOrDefaultAsync().AnyContext(); ;

                result = ToFindHit(response);

                if (IsCacheEnabled)
                    await SetCachedQueryResultAsync(options, result).AnyContext();

                return result;
            }
        }
        
        public async Task<T> GetByIdAsync(Id id, ICommandOptions options = null) {
            if (String.IsNullOrEmpty(id.Value))
                return null;

            T hit = null;
            if (IsCacheEnabled && options.ShouldReadCache())
                hit = await Cache.GetAsync<T>(id, default(T)).AnyContext();

            if (hit != null) {
                _logger.Trace(() => $"Cache hit: type={EntityTypeName} key={id}");
                return hit;
            }

            using (var session = _store.QuerySession())
                hit = await session.LoadAsync<T>(id.Value).AnyContext();

            if (hit != null && IsCacheEnabled && options.ShouldUseCache())
                await Cache.SetAsync(id, hit, options.GetExpiresIn()).AnyContext();

            return hit;
        }

        public async Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, ICommandOptions options = null) {
            var idList = ids?.Distinct().Where(i => !String.IsNullOrEmpty(i)).ToList();
            if (idList == null || idList.Count == 0)
                return EmptyList;

            if (!HasIdentity)
                throw new NotSupportedException("Model type must implement IIdentity.");

            var hits = new List<T>();
            if (IsCacheEnabled && options.ShouldReadCache()) {
                var cacheHits = await Cache.GetAllAsync<T>(idList.Select(id => id.Value)).AnyContext();
                hits.AddRange(cacheHits.Where(kvp => kvp.Value.HasValue).Select(kvp => kvp.Value.Value));
            }

            var itemsToFind = idList.Except(hits.OfType<IIdentity>().Select(i => (Id)i.Id)).ToList();
            if (itemsToFind.Count == 0)
                return hits.AsReadOnly();

            using (var session = _store.QuerySession()) {
                var docs = await session.LoadManyAsync<T>(ids.Select(i => i.Value).ToArray()).AnyContext();
                hits.AddRange(docs);
            }

            if (IsCacheEnabled && options.ShouldUseCache()) {
                foreach (var item in hits.OfType<IIdentity>())
                    await Cache.SetAsync(item.Id, item, options.GetExpiresIn()).AnyContext();
            }

            return hits.AsReadOnly();
        }

        public Task<FindResults<T>> GetAllAsync(ICommandOptions options = null) {
            return FindAsync(null, options);
        }

        public async Task<bool> ExistsAsync(Id id) {
            if (String.IsNullOrEmpty(id.Value))
                return false;
            
            using (var session = _store.QuerySession()) {
                var count = await session.Query<T>().CountAsync(d => ((IIdentity)d).Id == id.Value).AnyContext();
                return count > 0;
            }
        }

        public async Task<bool> ExistsAsync(IRepositoryQuery query) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var options = ConfigureOptions(null);
            await OnBeforeQueryAsync(query, options, typeof(T)).AnyContext();

            using (var session = _store.QuerySession()) {
                var response = await session.Query<T>(query, options).CountAsync().AnyContext();

                return response > 0;
            }
        }

        public async Task<CountResult> CountAsync(IRepositoryQuery query, ICommandOptions options = null) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var result = await GetCachedQueryResultAsync<CountResult>(options, "count").AnyContext();
            if (result != null)
                return result;

            options = ConfigureOptions(options);
            await OnBeforeQueryAsync(query, options, typeof(T)).AnyContext();

            using (var session = _store.QuerySession()) {
                var response = await session.Query<T>(query, options).CountAsync().AnyContext();

                result = new CountResult(response);
                await SetCachedQueryResultAsync(options, result, "count").AnyContext();
                return result;
            }
        }

        public async Task<long> CountAsync(ICommandOptions options = null) {
            using (var session = _store.QuerySession()) {
                var result = await session.Query<T>().CountAsync().AnyContext();
                return result;
            }
        }

        protected virtual IRepositoryQuery<T> NewQuery() {
            return new RepositoryQuery<T>();
        }

        protected virtual IRepositoryQuery ConfigureQuery(IRepositoryQuery query) {
            if (query == null)
                query = new RepositoryQuery<T>();

            if (DefaultExcludes.Count > 0 && query.GetExcludes().Count == 0)
                query.Exclude(DefaultExcludes);

            return query;
        }

        public bool IsCacheEnabled { get; private set; } = true;
        protected ScopedCacheClient Cache => _scopedCacheClient ?? new ScopedCacheClient(new NullCacheClient());

        private void SetCache(ICacheClient cache) {
            IsCacheEnabled = cache != null;
            _scopedCacheClient = new ScopedCacheClient(cache ?? new NullCacheClient(), EntityTypeName);
        }

        protected void DisableCache() {
            IsCacheEnabled = false;
            _scopedCacheClient = new ScopedCacheClient(new NullCacheClient(), EntityTypeName);
        }

        protected virtual async Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<T>> documents, ICommandOptions options) {
            if (!IsCacheEnabled)
                return;

            if (documents != null && documents.Count > 0 && HasIdentity) {
                var keys = documents
                    .Select(d => d.Value)
                    .Cast<IIdentity>()
                    .Select(d => d.Id)
                    .ToList();

                if (keys.Count > 0)
                    await Cache.RemoveAllAsync(keys).AnyContext();
            }
        }

        public Task InvalidateCacheAsync(T document, ICommandOptions options = null) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            if (!IsCacheEnabled)
                return TaskHelper.Completed();

            return InvalidateCacheAsync(new[] { document }, options);
        }

        public Task InvalidateCacheAsync(IEnumerable<T> documents, ICommandOptions options = null) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (!IsCacheEnabled)
                return TaskHelper.Completed();

            return InvalidateCacheAsync(docs.Select(d => new ModifiedDocument<T>(d, null)).ToList(), options);
        }

        protected ICommandOptions ConfigureOptions(ICommandOptions options) {
            if (options == null)
                options = new CommandOptions<T>();

            return options;
        }

        protected async Task<TResult> GetCachedQueryResultAsync<TResult>(ICommandOptions options, string cachePrefix = null, string cacheSuffix = null) {
            if (!IsCacheEnabled || options == null || !options.ShouldReadCache() || !options.HasCacheKey())
                return default(TResult);

            string cacheKey = cachePrefix != null ? cachePrefix + ":" + options.GetCacheKey() : options.GetCacheKey();
            if (!String.IsNullOrEmpty(cacheSuffix))
                cacheKey += ":" + cacheSuffix;

            var result = await Cache.GetAsync<TResult>(cacheKey, default(TResult)).AnyContext();
            _logger.Trace(() => $"Cache {(result != null ? "hit" : "miss")}: type={EntityTypeName} key={cacheKey}");

            return result;
        }

        protected async Task SetCachedQueryResultAsync<TResult>(ICommandOptions options, TResult result, string cachePrefix = null, string cacheSuffix = null) {
            if (!IsCacheEnabled || result == null || options == null || !options.ShouldUseCache() || !options.HasCacheKey())
                return;

            string cacheKey = cachePrefix != null ? cachePrefix + ":" + options.GetCacheKey() : options.GetCacheKey();
            if (!String.IsNullOrEmpty(cacheSuffix))
                cacheKey += ":" + cacheSuffix;

            await Cache.SetAsync(cacheKey, result, options.GetExpiresIn()).AnyContext();
            _logger.Trace(() => $"Set cache: type={EntityTypeName} key={cacheKey}");
        }

        #region Events

        public AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; } = new AsyncEvent<BeforeQueryEventArgs<T>>();

        private async Task OnBeforeQueryAsync(IRepositoryQuery query, ICommandOptions options, Type resultType) {
            if (BeforeQuery == null)
                return;

            await BeforeQuery.InvokeAsync(this, new BeforeQueryEventArgs<T>(query, options, this, resultType)).AnyContext();
        }

        #endregion
    }
}