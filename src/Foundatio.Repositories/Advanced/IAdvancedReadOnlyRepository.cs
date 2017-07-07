using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Advanced {
    public interface IAdvancedReadOnlyRepository<T> : IReadOnlyRepository<T> where T : class, new() {
        Task<FindResults<T>> FindAsync(IRepositoryQuery query, ICommandOptions options = null);
        Task<FindResults<TResult>> FindAsAsync<TResult>(IRepositoryQuery query, ICommandOptions options = null) where TResult : class, new();
        Task<FindHit<T>> FindOneAsync(IRepositoryQuery query, ICommandOptions options = null);
        Task<bool> ExistsAsync(IRepositoryQuery query);
        Task<CountResult> CountAsync(IRepositoryQuery query, ICommandOptions options = null);
    }

    public interface IAdvancedSearchableReadOnlyRepository<T> : ISearchableReadOnlyRepository<T>
        where T : class, new() {
    }

    public static class ReadOnlyRepositoryExtensions {
        public static Task<FindResults<T>> FindAsync<T>(this IAdvancedReadOnlyRepository<T> repository, RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.FindAsAsync<T>(query.Configure(), options.Configure());
        }

        public static Task<FindResults<TResult>> FindAsAsync<TModel, TResult>(this IAdvancedReadOnlyRepository<TModel> repository, RepositoryQueryDescriptor<TModel> query, CommandOptionsDescriptor<TModel> options = null) where TResult : class, new() where TModel : class, new() {
            return repository.FindAsAsync<TResult>(query.Configure(), options.Configure());
        }

        public static Task<FindHit<T>> FindOneAsync<T>(this IAdvancedReadOnlyRepository<T> repository, RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.FindOneAsync(query.Configure(), options.Configure());
        }

        public static Task<bool> ExistsAsync<T>(this IAdvancedReadOnlyRepository<T> repository, RepositoryQueryDescriptor<T> query) where T : class, new() {
            return repository.ExistsAsync(query.Configure());
        }

        public static Task<CountResult> CountAsync<T>(this IAdvancedReadOnlyRepository<T> repository, RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.CountAsync(query.Configure(), options.Configure());
        }
    }
}