using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Advanced
{
    public interface IAdvancedRepository<T> : IRepository<T> where T : class, IIdentity, new() {
        Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions options = null);
        Task<long> RemoveAllAsync(IRepositoryQuery query, ICommandOptions options = null);
        Task<long> BatchProcessAsync(IRepositoryQuery query, Func<FindResults<T>, Task<bool>> processAsync, ICommandOptions options = null);
        Task<long> BatchProcessAsAsync<TResult>(IRepositoryQuery query, Func<FindResults<TResult>, Task<bool>> processAsync, ICommandOptions options = null)
            where TResult : class, new();
    }

    public interface IAdvancedSearchableRepository<T> : ISearchableRepository<T> where T : class, IIdentity, new() { }

    public static class RepositoryExtensions {
        public static Task<long> PatchAllAsync<T>(this IAdvancedRepository<T> repository, RepositoryQueryDescriptor<T> query, IPatchOperation operation, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.PatchAllAsync(query.Configure(), operation, options.Configure());
        }

        public static Task<long> RemoveAllAsync<T>(this IAdvancedRepository<T> repository, RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.RemoveAllAsync(query.Configure(), options.Configure());
        }

        public static Task<long> BatchProcessAsync<T>(this IAdvancedRepository<T> repository, RepositoryQueryDescriptor<T> query, Func<FindResults<T>, Task<bool>> processAsync, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.BatchProcessAsync(query.Configure(), processAsync, options.Configure());
        }
    }
}