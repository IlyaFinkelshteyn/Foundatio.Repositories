using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Logging.Xunit;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Marten.Queries;
using Foundatio.Repositories.Marten.Queries.Builders;
using Foundatio.Repositories.Marten.Tests.Repositories.Models;
using Foundatio.Repositories.Marten.Tests.Repositories.Queries;
using Foundatio.Utility;
using Marten;
using Xunit.Abstractions;
using LogLevel = Foundatio.Logging.LogLevel;

namespace Foundatio.Repositories.Marten.Tests {
    public abstract class MartenRepositoryTestBase : TestWithLoggingBase {
        protected readonly InMemoryCacheClient _cache;
        protected readonly IDocumentStore _store;
        protected readonly IQueue<WorkItemData> _workItemQueue;
        protected readonly InMemoryMessageBus _messageBus;

        public MartenRepositoryTestBase(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;
            Log.SetLogLevel<ScheduledTimer>(LogLevel.Warning);

            _cache = new InMemoryCacheClient(new InMemoryCacheClientOptions { LoggerFactory = Log });
            _messageBus = new InMemoryMessageBus(new InMemoryMessageBusOptions { LoggerFactory = Log });
            _workItemQueue = new InMemoryQueue<WorkItemData>(new InMemoryQueueOptions<WorkItemData> { LoggerFactory = Log });
            _store = GetDocumentStore();

            MartenQueryBuilder.Default.Register<AgeQueryBuilder>();
            MartenQueryBuilder.Default.Register<CompanyQueryBuilder>();
        }

        private IDocumentStore GetDocumentStore() {
            var store = DocumentStore.For(st => {
                st.UseDefaultSerialization(EnumStorage.AsString, Casing.CamelCase);
                st.Connection("host=localhost;database=marten;username=postgres;password=banana");
                st.Logger(new MartenTestLogger(Log));
                st.Linq.MethodCallParsers.Add(new MatchesWhereFragmentParser());
                st.AutoCreateSchemaObjects = AutoCreate.All;
                st.Schema.For<Employee>().IndexLastModified().Duplicate(e => e.IsDeleted).UseOptimisticConcurrency(true);
                st.Schema.For<Identity>();
            });

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