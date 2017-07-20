using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Logging.Xunit;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Utility;
using Marten;
using Xunit.Abstractions;
using LogLevel = Foundatio.Logging.LogLevel;

namespace Foundatio.Repositories.Marten.Tests {
    public abstract class ElasticRepositoryTestBase : TestWithLoggingBase {
        protected readonly InMemoryCacheClient _cache;
        protected readonly IDocumentStore _store;
        protected readonly IQueue<WorkItemData> _workItemQueue;
        protected readonly InMemoryMessageBus _messageBus;

        public ElasticRepositoryTestBase(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;
            Log.SetLogLevel<ScheduledTimer>(LogLevel.Warning);

            _cache = new InMemoryCacheClient(new InMemoryCacheClientOptions { LoggerFactory = Log });
            _messageBus = new InMemoryMessageBus(new InMemoryMessageBusOptions { LoggerFactory = Log });
            _workItemQueue = new InMemoryQueue<WorkItemData>(new InMemoryQueueOptions<WorkItemData> { LoggerFactory = Log });
            _store = _configuration.Client;
        }

        protected virtual async Task RemoveDataAsync(bool configureIndexes = true) {
            var minimumLevel = Log.MinimumLevel;
            Log.MinimumLevel = LogLevel.Warning;

            await _workItemQueue.DeleteQueueAsync();
            _store.Advanced.Clean.DeleteAllDocuments();
            _store.Schema.ApplyAllConfiguredChangesToDatabase();

            await _cache.RemoveAllAsync();
            _messageBus.ResetMessagesSent();

            Log.MinimumLevel = minimumLevel;
        }
    }
}