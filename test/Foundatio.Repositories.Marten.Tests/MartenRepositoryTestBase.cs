using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Logging.Xunit;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Marten.Queries;
using Foundatio.Repositories.Marten.Tests.Repositories.Models;
using Foundatio.Utility;
using Marten;
using Xunit.Abstractions;
using LogLevel = Foundatio.Logging.LogLevel;

namespace Foundatio.Repositories.Marten.Tests {
    public abstract class MartenRepositoryTestBase : TestWithLoggingBase {
        protected readonly InMemoryCacheClient _cache;
        protected static readonly IDocumentStore _store;
        protected readonly IQueue<WorkItemData> _workItemQueue;
        protected readonly InMemoryMessageBus _messageBus;

        static MartenRepositoryTestBase() {
            _store = GetDocumentStore();
        }

        public MartenRepositoryTestBase(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;
            Log.SetLogLevel<ScheduledTimer>(LogLevel.Warning);

            _cache = new InMemoryCacheClient(new InMemoryCacheClientOptions { LoggerFactory = Log });
            _messageBus = new InMemoryMessageBus(new InMemoryMessageBusOptions { LoggerFactory = Log });
            _workItemQueue = new InMemoryQueue<WorkItemData>(new InMemoryQueueOptions<WorkItemData> { LoggerFactory = Log });
            
        }

        private static IDocumentStore GetDocumentStore() {
            var store = DocumentStore.For(st => {
                st.Connection("host=localhost;database=marten;username=postgres;password=banana");
                st.Logger(new MartenConsoleLogger());
                st.Linq.MethodCallParsers.Add(new MatchesWhereFragmentParser());

                st.Schema.For<Employee>().SoftDeleted();
            });

            store.Schema.ApplyAllConfiguredChangesToDatabase();

            return store;
        }

        protected virtual async Task RemoveDataAsync(bool configureIndexes = true) {
            var minimumLevel = Log.MinimumLevel;
            Log.MinimumLevel = LogLevel.Warning;

            await _workItemQueue.DeleteQueueAsync();
            _store.Advanced.Clean.DeleteAllDocuments();

            await _cache.RemoveAllAsync();
            _messageBus.ResetMessagesSent();

            Log.MinimumLevel = minimumLevel;
        }
    }
}