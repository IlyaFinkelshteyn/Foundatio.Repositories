using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Messaging;
using Foundatio.Repositories.Marten.Tests.Repositories.Models;
using Marten;

namespace Foundatio.Repositories.Marten.Tests {
    public class IdentityRepository : MartenRepositoryBase<Identity> {
        public IdentityRepository(IDocumentStore store, ICacheClient cacheClient, ILoggerFactory loggerFactory, IMessagePublisher messagePublisher) : base(store, cacheClient, loggerFactory, messagePublisher) {
        }
    }

    public class IdentityWithNoCachingRepository : IdentityRepository {
        public IdentityWithNoCachingRepository(IDocumentStore store, ICacheClient cacheClient, ILoggerFactory loggerFactory, IMessagePublisher messagePublisher) : base(store, cacheClient, loggerFactory, messagePublisher) {
            DisableCache();
        }
    }
}