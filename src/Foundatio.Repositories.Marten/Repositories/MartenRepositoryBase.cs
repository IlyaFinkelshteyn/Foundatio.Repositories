using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Baseline.Reflection;
using FluentValidation;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Messaging;
using Foundatio.Repositories.Advanced;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.JsonPatch;
using Foundatio.Repositories.Marten.Queries;
using Foundatio.Repositories.Marten.Queries.Builders;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Foundatio.Utility;
using Foundatio.Repositories.Options;
using Marten;
using Marten.Linq;
using Marten.Services;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ConcurrencyException = Foundatio.Repositories.Exceptions.ConcurrencyException;
using m = Marten;

namespace Foundatio.Repositories.Marten {
    public abstract class MartenRepositoryBase<T> : MartenReadOnlyRepositoryBase<T>, IAdvancedRepository<T> where T : class, IIdentity, new() {
        protected readonly IValidator<T> _validator;
        protected readonly IMessagePublisher _messagePublisher;

        protected MartenRepositoryBase(IDocumentStore store, ICacheClient cacheClient, ILoggerFactory loggerFactory, IMessagePublisher messagePublisher = null, IValidator<T> validator = null) : base(store, cacheClient, loggerFactory) {
            _validator = validator;
            _messagePublisher = messagePublisher;
            NotificationsEnabled = _messagePublisher != null;

            if (HasCreatedDate) {
                var field = _mapping.FieldFor(ReflectionHelper.GetProperty((T e) => ((IHaveCreatedDate)e).CreatedUtc));
                FieldsRequiredForRemove.Add(field.SqlLocator);
            }
        }

        public async Task<T> AddAsync(T document, ICommandOptions options = null) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            await AddAsync(new[] { document }, options).AnyContext();
            return document;
        }

        public async Task AddAsync(IEnumerable<T> documents, ICommandOptions options = null) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (docs.Count == 0)
                return;

            await OnDocumentsAddingAsync(docs, options).AnyContext();

            if (_validator != null)
                foreach (var doc in docs)
                    await _validator.ValidateAndThrowAsync(doc).AnyContext();

            await StoreDocumentsAsync(docs, true, options).AnyContext();
            await AddToCacheAsync(docs, options).AnyContext();

            await OnDocumentsAddedAsync(docs, options).AnyContext();
        }

        public async Task<T> SaveAsync(T document, ICommandOptions options = null) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            await SaveAsync(new[] { document }, options).AnyContext();
            return document;
        }

        public async Task SaveAsync(IEnumerable<T> documents, ICommandOptions options = null) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (docs.Count == 0)
                return;

            string[] ids = docs.Where(d => !String.IsNullOrEmpty(d.Id)).Select(d => d.Id).ToArray();
            if (ids.Length < docs.Count)
                throw new ApplicationException("Id must be set when calling Save.");

            options = ConfigureOptions(options);

            var originalDocuments = await GetOriginalDocumentsAsync(ids, options).AnyContext();
            await OnDocumentsSavingAsync(docs, originalDocuments, options).AnyContext();

            if (_validator != null)
                foreach (var doc in docs)
                    await _validator.ValidateAndThrowAsync(doc).AnyContext();

            await StoreDocumentsAsync(docs, false, options).AnyContext();
            await AddToCacheAsync(docs, options).AnyContext();

            await OnDocumentsSavedAsync(docs, originalDocuments, options).AnyContext();
        }

        private async Task<IReadOnlyCollection<T>> GetOriginalDocumentsAsync(Ids ids, ICommandOptions options) {
            if (!options.GetOriginalsEnabled(OriginalsEnabled) || ids.Count == 0)
                return EmptyList;

            var originals = options.GetOriginals<T>().ToList();
            foreach (var original in originals)
                ids.RemoveAll(id => id.Value == original.Id);

            originals.AddRange(await GetByIdsAsync(ids, options.Clone().ReadCache()).AnyContext());

            return originals.AsReadOnly();
        }

        public Task PatchAsync(Id id, IPatchOperation operation, ICommandOptions options = null) {
            if (String.IsNullOrEmpty(id.Value))
                throw new ArgumentNullException(nameof(id));

            if (operation == null)
                throw new ArgumentNullException(nameof(operation));

            return PatchAllAsync(NewQuery().Id(id), operation, options);
        }

        public async Task PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions options = null) {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));

            if (operation == null)
                throw new ArgumentNullException(nameof(operation));

            if (ids.Count == 0)
                return;

            if (ids.Count == 1) {
                await PatchAsync(ids[0], operation, options).AnyContext();
                return;
            }

            await PatchAllAsync(NewQuery().Id(ids), operation, options).AnyContext();
        }

        public async Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions options = null) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (operation == null)
                throw new ArgumentNullException(nameof(operation));

            options = ConfigureOptions(options);

            long affectedRecords = 0;
            if (operation is Models.JsonPatch jsonOperation) {
                var patcher = new JsonPatcher();
                affectedRecords += await BatchProcessAsAsync<JObject>(query, async results => {
                    using (var session = _store.OpenSession(new SessionOptions { ConcurrencyChecks = ConcurrencyChecks.Disabled, Tracking = DocumentTracking.IdentityOnly})) {
                        foreach (var h in results.Hits) {
                            var target = h.Document as JToken;
                            patcher.Patch(ref target, jsonOperation.Patch);
                            session.Store(target.ToObject<T>());
                        }

                        await session.SaveChangesAsync().AnyContext();
                    }

                    var updatedIds = results.Hits.Select(h => h.Id).ToList();
                    if (IsCacheEnabled)
                        await Cache.RemoveAllAsync(updatedIds).AnyContext();

                    try {
                        options.GetUpdatedIdsCallback()?.Invoke(updatedIds);
                    }
                    catch (Exception ex) {
                        _logger.Error(ex, "Error calling updated ids callback.");
                    }

                    return true;
                }, options.Clone()).AnyContext();
            }
            else {
                var scriptOperation = operation as ScriptPatch;
                var partialOperation = operation as PartialPatch;
                if (scriptOperation == null && partialOperation == null)
                    throw new ArgumentException("Unknown operation type", nameof(operation));

                if (scriptOperation != null)
                    throw new ArgumentException("Script operation not supported", nameof(operation));

                if (!query.GetIncludes().Contains(_idField))
                    query.Include(_idField);

                affectedRecords += await BatchProcessAsAsync<JObject>(query, async results => {
                    using (var session = _store.OpenSession(new SessionOptions { ConcurrencyChecks = ConcurrencyChecks.Disabled, Tracking = DocumentTracking.IdentityOnly })) {
                        foreach (var h in results.Hits) {
                            var partialJson = JObject.Parse(_store.Serializer.ToJson(partialOperation.Document));
                            h.Document.Merge(partialJson);
                            session.Store(h.Document.ToObject<T>());
                        }

                        await session.SaveChangesAsync().AnyContext();
                    }

                    var updatedIds = results.Hits.Select(h => h.Id).ToList();
                    if (IsCacheEnabled)
                        await Cache.RemoveAllAsync(updatedIds).AnyContext();

                    try {
                        options.GetUpdatedIdsCallback()?.Invoke(updatedIds);
                    }
                    catch (Exception ex) {
                        _logger.Error(ex, "Error calling updated ids callback.");
                    }

                    return true;

                }, options).AnyContext();
            }

            if (affectedRecords > 0) {
                // TODO: Find a good way to invalidate cache and send changed notification
                await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
                await SendQueryNotificationsAsync(ChangeType.Saved, query, options).AnyContext();
            }

            return affectedRecords;
        }

        public Task RemoveAsync(Id id, ICommandOptions options = null) {
            if (String.IsNullOrEmpty(id))
                throw new ArgumentNullException(nameof(id));

            return RemoveAsync((Ids)id, options);
        }

        public async Task RemoveAsync(Ids ids, ICommandOptions options = null) {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));

            if (options == null)
                options = new CommandOptions();

            if (IsCacheEnabled)
                options = options.ReadCache();

            // TODO: If not OriginalsEnabled then just delete by id
            // TODO: Delete by id using GetIndexById and id.Routing if its a child doc
            var documents = await GetByIdsAsync(ids, options).AnyContext();
            if (documents == null)
                return;

            await RemoveAsync(documents, options).AnyContext();
        }

        public Task RemoveAsync(T document, ICommandOptions options = null) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            return RemoveAsync(new[] { document }, options);
        }

        public async Task RemoveAsync(IEnumerable<T> documents, ICommandOptions options = null) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (docs.Count == 0)
                return;

            await OnDocumentsRemovingAsync(docs, options).AnyContext();

            using (var session = _store.LightweightSession()) {
                foreach (var d in docs)
                    session.Delete<T>(d.Id);

                await session.SaveChangesAsync().AnyContext();
            }

            await OnDocumentsRemovedAsync(docs, options).AnyContext();
        }

        public async Task<long> RemoveAllAsync(ICommandOptions options = null) {
            if (IsCacheEnabled)
                await Cache.RemoveAllAsync().AnyContext();

            return await RemoveAllAsync(NewQuery(), options).AnyContext();
        }
        
        protected List<QueryField> FieldsRequiredForRemove { get; } = new List<QueryField>();

        public async Task<long> RemoveAllAsync(IRepositoryQuery query, ICommandOptions options = null) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            options = ConfigureOptions(options);
            if (IsCacheEnabled && options.ShouldUseCache(true)) {
                foreach (var field in FieldsRequiredForRemove.Union(new QueryField[] { _idField }))
                    if (!query.GetIncludes().Contains(field))
                        query.Include(field);

                // TODO: What if you only want to send one notification?
                return await BatchProcessAsync(query, async results => {
                    await RemoveAsync(results.Documents, options).AnyContext();
                    return true;
                }, options.Clone()).AnyContext();
            }

            using (var session = _store.LightweightSession()) {
                var ctx = await MartenQueryBuilder.Default.BuildAsync<T>(query, options, _store).AnyContext();
                session.DeleteWhere<T>(d => d.MatchesWhereFragment(new CompoundWhereFragment("and", ctx.WhereFragments.ToArray())));
                await session.SaveChangesAsync().AnyContext();

                return -1;
            }
        }

        public Task<long> BatchProcessAsync(IRepositoryQuery query, Func<FindResults<T>, Task<bool>> processAsync, ICommandOptions options = null) {
            return BatchProcessAsAsync(query, processAsync, options);
        }

        public async Task<long> BatchProcessAsAsync<TResult>(IRepositoryQuery query, Func<FindResults<TResult>, Task<bool>> processAsync, ICommandOptions options = null)
            where TResult : class, new() {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (processAsync == null)
                throw new ArgumentNullException(nameof(processAsync));

            options = ConfigureOptions(options);
            if (!options.HasPageLimit())
                options.PageLimit(500);

            IReadOnlyList<string> ids;
            using (var session = _store.QuerySession())
                ids = await session.Query<T>(query, null).Select(d => d.Id).ToListAsync();

            long recordsProcessed = 0;
            var results = await FindAsAsync<TResult>(query, options).AnyContext();
            do {
                if (results.Hits.Count == 0)
                    break;

                // TODO: We need a generic way to do bulk operations and do exponential backoffs when we encounter on 429's (bulk queue is full). https://github.com/elastic/elasticsearch-net/pull/2162
                if (await processAsync(results).AnyContext()) {
                    recordsProcessed += results.Documents.Count;
                    continue;
                }

                _logger.Trace("Aborted batch processing.");
                break;
            } while (await results.NextPageAsync().AnyContext());

            _logger.Trace("{0} records processed", recordsProcessed);
            return recordsProcessed;
        }

        #region Events

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdding { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsAddingAsync(IReadOnlyCollection<T> documents, ICommandOptions options) {
            if (HasDates)
                documents.OfType<IHaveDates>().SetDates();
            else if (HasCreatedDate)
                documents.OfType<IHaveCreatedDate>().SetCreatedDates();

            documents.EnsureIds();

            if (DocumentsAdding != null)
                await DocumentsAdding.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Added, documents, options).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdded { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsAddedAsync(IReadOnlyCollection<T> documents, ICommandOptions options) {
            if (DocumentsAdded != null)
                await DocumentsAdded.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

            var modifiedDocs = documents.Select(d => new ModifiedDocument<T>(d, null)).ToList();
            await OnDocumentsChangedAsync(ChangeType.Added, modifiedDocs, options).AnyContext();
            await SendNotificationsAsync(ChangeType.Added, modifiedDocs, options).AnyContext();
        }

        public AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaving { get; } = new AsyncEvent<ModifiedDocumentsEventArgs<T>>();

        private async Task OnDocumentsSavingAsync(IReadOnlyCollection<T> documents, IReadOnlyCollection<T> originalDocuments, ICommandOptions options) {
            if (documents.Count == 0)
                return;

            if (HasDates)
                documents.Cast<IHaveDates>().SetDates();

            documents.EnsureIds();

            var modifiedDocs = originalDocuments.FullOuterJoin(
                documents, cf => cf.Id, cf => cf.Id,
                (original, modified, id) => new { Id = id, Original = original, Modified = modified }).Select(m => new ModifiedDocument<T>(m.Modified, m.Original)).ToList();

            await InvalidateCacheAsync(modifiedDocs, options).AnyContext();

            if (DocumentsSaving != null)
                await DocumentsSaving.InvokeAsync(this, new ModifiedDocumentsEventArgs<T>(modifiedDocs, this, options)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Saved, modifiedDocs, options).AnyContext();
        }

        public AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaved { get; } = new AsyncEvent<ModifiedDocumentsEventArgs<T>>();

        private async Task OnDocumentsSavedAsync(IReadOnlyCollection<T> documents, IReadOnlyCollection<T> originalDocuments, ICommandOptions options) {
            var modifiedDocs = originalDocuments.FullOuterJoin(
                documents, cf => cf.Id, cf => cf.Id,
                (original, modified, id) => new { Id = id, Original = original, Modified = modified }).Select(m => new ModifiedDocument<T>(m.Modified, m.Original)).ToList();

            if (SupportsSoftDeletes && IsCacheEnabled) {
                var deletedIds = modifiedDocs.Where(d => ((ISupportSoftDeletes)d.Value).IsDeleted).Select(m => m.Value.Id).ToArray();
                if (deletedIds.Length > 0)
                    await Cache.SetAddAsync("deleted", deletedIds, TimeSpan.FromSeconds(30)).AnyContext();

                var undeletedIds = modifiedDocs.Where(d => ((ISupportSoftDeletes)d.Value).IsDeleted == false).Select(m => m.Value.Id).ToArray();
                if (undeletedIds.Length > 0)
                    await Cache.SetRemoveAsync("deleted", undeletedIds, TimeSpan.FromSeconds(30)).AnyContext();
            }

            if (DocumentsSaved != null)
                await DocumentsSaved.InvokeAsync(this, new ModifiedDocumentsEventArgs<T>(modifiedDocs, this, options)).AnyContext();

            await OnDocumentsChangedAsync(ChangeType.Saved, modifiedDocs, options).AnyContext();
            await SendNotificationsAsync(ChangeType.Saved, modifiedDocs, options).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoving { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsRemovingAsync(IReadOnlyCollection<T> documents, ICommandOptions options) {
            await InvalidateCacheAsync(documents, options).AnyContext();

            if (DocumentsRemoving != null)
                await DocumentsRemoving.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Removed, documents, options).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoved { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsRemovedAsync(IReadOnlyCollection<T> documents, ICommandOptions options) {
            if (DocumentsRemoved != null)
                await DocumentsRemoved.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

            await OnDocumentsChangedAsync(ChangeType.Removed, documents, options).AnyContext();
            await SendNotificationsAsync(ChangeType.Removed, documents, options).AnyContext();
        }

        public AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanging { get; } = new AsyncEvent<DocumentsChangeEventArgs<T>>();

        private Task OnDocumentsChangingAsync(ChangeType changeType, IReadOnlyCollection<T> documents, ICommandOptions options) {
            return OnDocumentsChangingAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList(), options);
        }

        private async Task OnDocumentsChangingAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, ICommandOptions options) {
            if (DocumentsChanging == null)
                return;

            await DocumentsChanging.InvokeAsync(this, new DocumentsChangeEventArgs<T>(changeType, documents, this, options)).AnyContext();
        }

        public AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanged { get; } = new AsyncEvent<DocumentsChangeEventArgs<T>>();

        private Task OnDocumentsChangedAsync(ChangeType changeType, IReadOnlyCollection<T> documents, ICommandOptions options) {
            return OnDocumentsChangedAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList(), options);
        }

        private async Task OnDocumentsChangedAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, ICommandOptions options) {
            if (DocumentsChanged == null)
                return;

            await DocumentsChanged.InvokeAsync(this, new DocumentsChangeEventArgs<T>(changeType, documents, this, options)).AnyContext();
        }

        #endregion

        private async Task StoreDocumentsAsync(IReadOnlyCollection<T> documents, bool isCreateOperation, ICommandOptions options) {
            using (var session = _store.LightweightSession()) {
                foreach (var doc in documents) {
                    if (isCreateOperation) {
                        session.Insert(doc);
                    } else {
                        if (HasVersion)
                            session.Store(doc, ((IVersioned)doc).GetVersionAsGuidOrDefault());
                        else
                            session.Store(doc);
                    }
                }

                try {
                    await session.SaveChangesAsync();
                } catch (MartenCommandException ex) {
                    if (ex.InnerException is Npgsql.PostgresException inner && inner.SqlState == "23505")
                        throw new DuplicateDocumentException(inner.Message, ex);

                    throw;
                } catch (AggregateException ex) {
                    if (ex.InnerExceptions.Count == 1 && ex.InnerExceptions.Any(e => e is m.Services.ConcurrencyException))
                        throw new ConcurrencyException("Document version does not match.", ex);

                    throw;
                }

                SetVersions(documents, session);
            }
        }

        private Exception TranslateException(Exception ex) {
            return ex;
        }

        protected virtual async Task AddToCacheAsync(ICollection<T> documents, ICommandOptions options) {
            if (!IsCacheEnabled || Cache == null || !options.ShouldUseCache())
                return;

            foreach (var document in documents)
                await Cache.SetAsync(document.Id, document, options.GetExpiresIn()).AnyContext();
        }

        protected bool NotificationsEnabled { get; set; }
        protected bool OriginalsEnabled { get; set; }
        public bool BatchNotifications { get; set; }

        private Task SendNotificationsAsync(ChangeType changeType, ICommandOptions options) {
            return SendNotificationsAsync(changeType, EmptyList, options);
        }

        private Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<T> documents, ICommandOptions options) {
            return SendNotificationsAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList(), options);
        }

        protected virtual async Task SendQueryNotificationsAsync(ChangeType changeType, IRepositoryQuery query, ICommandOptions options) {
            if (!NotificationsEnabled || !options.ShouldNotify())
                return;

            var delay = TimeSpan.FromSeconds(1.5);
            var ids = query.GetIds();
            if (ids.Count > 0) {
                foreach (string id in ids) {
                    await PublishMessageAsync(new EntityChanged {
                        ChangeType = changeType,
                        Id = id,
                        Type = EntityTypeName
                    }, delay).AnyContext();
                }

                return;
            }

            await PublishMessageAsync(new EntityChanged {
                ChangeType = changeType,
                Type = EntityTypeName
            }, delay).AnyContext();
        }

        protected virtual async Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, ICommandOptions options) {
            if (!NotificationsEnabled || !options.ShouldNotify())
                return;

            var delay = TimeSpan.FromSeconds(1.5);

            if (documents.Count == 0) {
                await PublishChangeTypeMessageAsync(changeType, null, delay).AnyContext();
            }
            else if (BatchNotifications && documents.Count > 1) {
                // TODO: This needs to support batch notifications
                if (!SupportsSoftDeletes || changeType != ChangeType.Saved) {
                    foreach (var doc in documents.Select(d => d.Value)) {
                        await PublishChangeTypeMessageAsync(changeType, doc, delay).AnyContext();
                    }

                    return;
                }
                bool allDeleted = documents.All(d => d.Original != null && ((ISupportSoftDeletes)d.Original).IsDeleted == false && ((ISupportSoftDeletes)d.Value).IsDeleted);
                foreach (var doc in documents.Select(d => d.Value)) {
                    await PublishChangeTypeMessageAsync(allDeleted ? ChangeType.Removed : changeType, doc, delay).AnyContext();
                }
            }
            else {
                if (!SupportsSoftDeletes) {
                    foreach (var d in documents)
                        await PublishChangeTypeMessageAsync(changeType, d.Value, delay).AnyContext();

                    return;
                }

                foreach (var d in documents) {
                    var docChangeType = changeType;
                    if (d.Original != null) {
                        var document = (ISupportSoftDeletes)d.Value;
                        var original = (ISupportSoftDeletes)d.Original;
                        if (original.IsDeleted == false && document.IsDeleted)
                            docChangeType = ChangeType.Removed;
                    }

                    await PublishChangeTypeMessageAsync(docChangeType, d.Value, delay).AnyContext();
                }
            }
        }

        protected Task PublishChangeTypeMessageAsync(ChangeType changeType, T document, TimeSpan delay) {
            return PublishChangeTypeMessageAsync(changeType, document, null, delay);
        }

        protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, T document, IDictionary<string, object> data = null, TimeSpan? delay = null) {
            return PublishChangeTypeMessageAsync(changeType, document?.Id, data, delay);
        }

        protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, string id, IDictionary<string, object> data = null, TimeSpan? delay = null) {
            return PublishMessageAsync(new EntityChanged {
                ChangeType = changeType,
                Id = id,
                Type = EntityTypeName,
                Data = new DataDictionary(data ?? new Dictionary<string, object>())
            }, delay);
        }

        protected Task PublishMessageAsync<TMessageType>(TMessageType message, TimeSpan? delay = null) where TMessageType : class {
            if (_messagePublisher == null)
                return Task.CompletedTask;

            return _messagePublisher.PublishAsync(message, delay);
        }
    }
}