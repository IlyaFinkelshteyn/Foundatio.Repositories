using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Marten.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.JsonPatch;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Nito.AsyncEx;
using Xunit;
using Xunit.Abstractions;
using LogLevel = Foundatio.Logging.LogLevel;

namespace Foundatio.Repositories.Marten.Tests {
    public sealed class RepositoryTests : MartenRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;
        private readonly IdentityRepository _identityRepository;
        private readonly IdentityRepository _identityRepositoryWithNoCaching;

        public RepositoryTests(ITestOutputHelper output) : base(output) {
            _employeeRepository = new EmployeeRepository(_store, _cache, Log, _messageBus);
            _identityRepository = new IdentityRepository(_store, _cache, Log, _messageBus);
            _identityRepositoryWithNoCaching = new IdentityWithNoCachingRepository(_store, _cache, Log, _messageBus);

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task AddAsync() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Generate());
            Assert.NotNull(identity1?.Id);

            var disposables = new List<IDisposable>(2);
            var countdownEvent = new AsyncCountdownEvent(2);

            try {
                var identity2 = IdentityGenerator.Default;
                disposables.Add(_identityRepository.DocumentsAdding.AddSyncHandler((o, args) => {
                    Assert.Equal(identity2, args.Documents.First());
                    countdownEvent.Signal();
                }));

                disposables.Add(_identityRepository.DocumentsAdded.AddSyncHandler((o, args) => {
                    Assert.Equal(identity2, args.Documents.First());
                    countdownEvent.Signal();
                }));

                var result = await _identityRepository.AddAsync(identity2);
                Assert.Equal(IdentityGenerator.Default.Id, result.Id);

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task CanQueryByDeleted() {
            var employee1 = EmployeeGenerator.Default;
            employee1.IsDeleted = true;
            employee1 = await _employeeRepository.AddAsync(employee1, o => o.ImmediateConsistency());
            Assert.NotNull(employee1?.Id);

            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

            var allEmployees = await _employeeRepository.GetByQueryAsync(q => q.SoftDeleteMode(SoftDeleteQueryMode.All));
            Assert.Equal(2, allEmployees.Total);

            var onlyDeleted = await _employeeRepository.GetByQueryAsync(q => q.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
            Assert.Equal(1, onlyDeleted.Total);
            Assert.Equal(employee1.Id, onlyDeleted.Documents.First().Id);

            var nonDeletedEmployees = await _employeeRepository.GetByQueryAsync(q => q.SoftDeleteMode(SoftDeleteQueryMode.ActiveOnly));
            Assert.Equal(1, nonDeletedEmployees.Total);
            Assert.NotEqual(employee1.Id, nonDeletedEmployees.Documents.First().Id);
        }

        [Fact]
        public async Task CanQueryByDeletedSearch() {
            var employee1 = EmployeeGenerator.Default;
            employee1.IsDeleted = true;
            employee1 = await _employeeRepository.AddAsync(employee1, o => o.ImmediateConsistency());
            Assert.NotNull(employee1?.Id);

            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

            var allEmployees = await _employeeRepository.FindAsync(new RepositoryQuery().SoftDeleteMode(SoftDeleteQueryMode.All));
            Assert.Equal(2, allEmployees.Total);

            var onlyDeleted = await _employeeRepository.FindAsync(new RepositoryQuery().SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
            Assert.Equal(1, onlyDeleted.Total);
            Assert.Equal(employee1.Id, onlyDeleted.Documents.First().Id);

            var nonDeletedEmployees = await _employeeRepository.GetAllAsync();
            Assert.Equal(1, nonDeletedEmployees.Total);
            Assert.NotEqual(employee1.Id, nonDeletedEmployees.Documents.First().Id);
        }

        [Fact]
        public async Task AddDuplicateAsync() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
            Assert.NotNull(identity1?.Id);

            await Assert.ThrowsAsync<DuplicateDocumentException>(async () => await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency()));
            Assert.Equal(1, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task AddDuplicateCollectionAsync() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
            Assert.NotNull(identity1?.Id);

            var identities = new List<Identity> {
                IdentityGenerator.Default,
                IdentityGenerator.Generate()
            };

            await Assert.ThrowsAsync<DuplicateDocumentException>(async () => await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency()));
            Assert.Equal(1, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task AddWithCachingAsync() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.Cache());
            Assert.NotNull(identity?.Id);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
        }

        [Fact]
        public async Task AddCollectionAsync() {
            var identity = IdentityGenerator.Generate();
            await _identityRepository.AddAsync(new List<Identity> { identity });
            Assert.NotNull(identity.Id);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id));
        }

        [Fact]
        public async Task AddCollectionWithCachingAsync() {
            var identity = IdentityGenerator.Generate();
            await _identityRepository.AddAsync(new List<Identity> { identity, IdentityGenerator.Generate() }, o => o.Cache());
            Assert.NotNull(identity?.Id);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
            Assert.Equal(2, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
        }

        [Fact]
        public async Task SaveWithOriginalFromOptions() {
            var original = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.Notifications(false));
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
            Assert.Equal(0, _messageBus.MessagesSent);
            Assert.Equal(0, _employeeRepository.QueryCount);

            var copy = original.DeepClone();
            copy.Age = 30;
            await _employeeRepository.SaveAsync(copy, o => o.AddOriginals(original));

            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
            Assert.Equal(1, _messageBus.MessagesSent);
            Assert.Equal(0, _employeeRepository.QueryCount);
        }

        [Fact]
        public async Task AddAndSaveWithCacheAsync() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.Cache());
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            string cacheKey = _cache.Keys.Single();
            var cacheValue = await _cache.GetAsync<Identity>(cacheKey);
            Assert.True(cacheValue.HasValue);
            Assert.Equal(identity, cacheValue.Value);

            identity = await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache());
            Assert.NotNull(identity);
            Assert.Equal(2, _cache.Hits);

            cacheValue = await _cache.GetAsync<Identity>(cacheKey);
            Assert.True(cacheValue.HasValue);
            Assert.Equal(identity, cacheValue.Value);

            await _identityRepository.InvalidateCacheAsync(identity);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(3, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            var result = await _identityRepository.SaveAsync(identity, o => o.Cache());
            Assert.NotNull(result);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(3, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            cacheValue = await _cache.GetAsync<Identity>(cacheKey);
            Assert.True(cacheValue.HasValue);
            Assert.Equal(identity, cacheValue.Value);
        }

        [Fact]
        public async Task SaveWithNoIdentityAsync() {
            var identity = IdentityGenerator.Generate();
            await Assert.ThrowsAsync<ApplicationException>(async () => await _identityRepository.SaveAsync(new List<Identity> { identity }, o => o.Cache()));
        }

        [Fact]
        public async Task SaveWithCachingAsync() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.Cache());
            Assert.NotNull(identity?.Id);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.InvalidateCacheAsync(identity);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            identity = await _identityRepository.SaveAsync(identity, o => o.Cache());
            Assert.NotNull(identity?.Id);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
        }

        [Fact]
        public async Task SaveCollectionAsync() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate(ObjectId.GenerateNewId().ToString()) };
            await _identityRepository.SaveAsync(identities);

            var results = await _identityRepository.GetByIdsAsync(identities.Select(i => i.Id).ToList());
            Assert.Equal(2, results.Count);
        }

        [Fact]
        public async Task SaveCollectionWithCachingAsync() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, o => o.Cache());
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.InvalidateCacheAsync(identities);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.SaveAsync(identities, o => o.Cache());
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            var results = await _identityRepository.GetByIdsAsync(new Ids(identities.Select(i => i.Id)), o => o.Cache());
            Assert.Equal(2, results.Count);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(2, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
        }

        [Fact]
        public async Task SetCreatedAndModifiedTimesAsync() {
            using (TestSystemClock.Install()) {
                SystemClock.Test.SubtractTime(TimeSpan.FromMilliseconds(100));
                DateTime nowUtc = SystemClock.UtcNow;
                var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
                Assert.True(employee.CreatedUtc >= nowUtc);
                Assert.True(employee.UpdatedUtc >= nowUtc);

                DateTime createdUtc = employee.CreatedUtc;
                DateTime updatedUtc = employee.UpdatedUtc;

                employee.Name = Guid.NewGuid().ToString();
                SystemClock.Test.AddTime(TimeSpan.FromMilliseconds(100));
                employee = await _employeeRepository.SaveAsync(employee);
                Assert.Equal(createdUtc, employee.CreatedUtc);
                Assert.True(updatedUtc < employee.UpdatedUtc, $"Previous UpdatedUtc: {updatedUtc} Current UpdatedUtc: {employee.UpdatedUtc}");
            }
        }

        [Fact]
        public async Task CannotSetFutureCreatedAndModifiedTimesAsync() {
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(createdUtc: DateTime.MaxValue, updatedUtc: DateTime.MaxValue));
            Assert.True(employee.CreatedUtc != DateTime.MaxValue);
            Assert.True(employee.UpdatedUtc != DateTime.MaxValue);

            employee.CreatedUtc = DateTime.MaxValue;
            employee.UpdatedUtc = DateTime.MaxValue;

            employee = await _employeeRepository.SaveAsync(employee);
            Assert.True(employee.CreatedUtc != DateTime.MaxValue);
            Assert.True(employee.UpdatedUtc != DateTime.MaxValue);
        }

        [Fact]
        public async Task JsonPatchAsync() {
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = "Patched" });
            await _employeeRepository.PatchAsync(employee.Id, new Models.JsonPatch(patch));

            employee = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
            Assert.Equal("Patched", employee.Name);
            Assert.NotEqual(Guid.Empty, employee.Version);
        }

        [Fact]
        public async Task PartialPatchAsync() {
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            await _employeeRepository.PatchAsync(employee.Id, new PartialPatch(new { name = "Patched" }));

            employee = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
            Assert.Equal("Patched", employee.Name);
            Assert.NotEqual(Guid.Empty, employee.Version);
        }

        [Fact]
        public async Task ScriptPatchAsync() {
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            await _employeeRepository.PatchAsync(employee.Id, new ScriptPatch("ctx._source.name = 'Patched';"));

            employee = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
            Assert.Equal("Patched", employee.Name);
            Assert.NotEqual(Guid.Empty, employee.Version);
        }

        [Fact]
        public async Task RemoveByIdsWithCachingAsync() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.RemoveAsync(identity.Id, o => o.ImmediateConsistency());
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(1, _cache.Misses);

            identity = await _identityRepository.AddAsync(IdentityGenerator.Generate(), o => o.Cache().ImmediateConsistency());
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(1, _cache.Misses);

            await _identityRepository.RemoveAsync(identity.Id, o => o.ImmediateConsistency());
            Assert.Equal(0, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(1, _cache.Misses);

            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveWithCachingAsync() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, o => o.Cache().ImmediateConsistency());
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.RemoveAsync(identities.First(), o => o.ImmediateConsistency());
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.RemoveAsync(identities, o => o.ImmediateConsistency());
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveCollectionAsync() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency());
            await _identityRepository.RemoveAsync(identities, o => o.ImmediateConsistency());

            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveCollectionWithCachingAsync() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, o => o.Cache().ImmediateConsistency());
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.RemoveAsync(identities, o => o.ImmediateConsistency());
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveAllAsync() {
            await _identityRepository.RemoveAllAsync();

            var identities = new List<Identity> { IdentityGenerator.Default };
            await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency());

            var disposables = new List<IDisposable>(2);
            var countdownEvent = new AsyncCountdownEvent(2);

            try {
                disposables.Add(_identityRepository.DocumentsRemoving.AddSyncHandler((o, args) => {
                    countdownEvent.Signal();
                }));
                disposables.Add(_identityRepository.DocumentsRemoved.AddSyncHandler((o, args) => {
                    countdownEvent.Signal();
                }));

                await _identityRepository.RemoveAllAsync(o => o.ImmediateConsistency());
                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);

            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }

            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveAllWithBatchingAsync() {
            const int COUNT = 1000;
            Log.SetLogLevel<IdentityRepository>(LogLevel.Information);
            var identities = IdentityGenerator.GenerateIdentities(COUNT);
            identities.EnsureIds();
            _store.BulkInsertDocuments(identities);
            Log.SetLogLevel<IdentityRepository>(LogLevel.Trace);

            var disposables = new List<IDisposable>(2);
            var countdownEvent = new AsyncCountdownEvent(COUNT * 2);

            try {
                disposables.Add(_identityRepository.DocumentsRemoving.AddSyncHandler((o, args) => {
                    countdownEvent.Signal(args.Documents.Count);
                }));
                disposables.Add(_identityRepository.DocumentsRemoved.AddSyncHandler((o, args) => {
                    countdownEvent.Signal(args.Documents.Count);
                }));

                var sw = Stopwatch.StartNew();
                Assert.Equal(COUNT, await _identityRepository.RemoveAllAsync(o => o.ImmediateConsistency()));
                sw.Stop();
                _logger.Info($"Deleted {COUNT} documents in {sw.ElapsedMilliseconds}ms");

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);
                Assert.Equal(0, await _identityRepository.CountAsync());
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task RemoveAllWithDeleteByQueryAsync() {
            const int COUNT = 10000;
            Log.SetLogLevel<IdentityWithNoCachingRepository>(LogLevel.Information);
            await _identityRepositoryWithNoCaching.AddAsync(IdentityGenerator.GenerateIdentities(COUNT), o => o.ImmediateConsistency());
            Log.SetLogLevel<IdentityWithNoCachingRepository>(LogLevel.Trace);

            var disposables = new List<IDisposable>(2);
            var countdownEvent = new AsyncCountdownEvent(1);

            try {
                disposables.Add(_identityRepositoryWithNoCaching.DocumentsRemoving.AddSyncHandler((o, args) => {
                    countdownEvent.Signal();
                }));
                disposables.Add(_identityRepositoryWithNoCaching.DocumentsRemoved.AddSyncHandler((o, args) => {
                    countdownEvent.Signal();
                }));

                var sw = Stopwatch.StartNew();
                Assert.Equal(COUNT, await _identityRepositoryWithNoCaching.RemoveAllAsync(o => o.ImmediateConsistency(true)));
                sw.Stop();
                _logger.Info($"Deleted {COUNT} documents in {sw.ElapsedMilliseconds}ms");

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);

                Assert.Equal(0, await _identityRepositoryWithNoCaching.CountAsync());
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task RemoveAllWithCachingAsync() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, o => o.Cache().ImmediateConsistency());
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.RemoveAllAsync(o => o.ImmediateConsistency());
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(0, await _identityRepository.CountAsync());
        }
    }
}