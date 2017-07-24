using Foundatio.Repositories.Marten.Tests.Repositories.Models;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Marten.Tests {
    public sealed class QueryTests : MartenRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;

        public QueryTests(ITestOutputHelper output) : base(output) {
            
            _employeeRepository = new EmployeeRepository(_store, _cache, Log, _messageBus);

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task GetByAgeAsync() {
            var employee19 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19), o => o.ImmediateConsistency());
            var employee20 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

            var results = await _employeeRepository.GetAllByAgeAsync(employee19.Age);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee19, results.Documents.First());

            results = await _employeeRepository.GetAllByAgeAsync(employee20.Age);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee20, results.Documents.First());
        }

        [Fact]
        public async Task GetByCompanyAsync() {
            var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());
            var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

            var results = await _employeeRepository.GetAllByCompanyAsync(employee1.CompanyId);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee1, results.Documents.First());

            results = await _employeeRepository.GetAllByCompanyAsync(employee2.CompanyId);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee2, results.Documents.First());

            results = await _employeeRepository.GetAllByCompaniesWithFieldEqualsAsync(new string[] { employee1.CompanyId });
            Assert.Equal(1, results.Total);

            results = await _employeeRepository.GetAllByCompaniesWithFieldEqualsAsync(new string[] { employee1.CompanyId, employee2.CompanyId });
            Assert.Equal(2, results.Total);

            Assert.Equal(1, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));
            await _employeeRepository.RemoveAsync(employee1, o => o.Cache().ImmediateConsistency());

           await  _employeeRepository.GetByQueryAsync(q => q.FieldCondition(e => e.Age, ComparisonOperator.Equals, 12));
            Assert.Equal(1, await _employeeRepository.CountAsync());
            Assert.Equal(0, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));
        }

        [Fact]
        public async Task GetByMissingFieldAsync() {
            var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());
            var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyName: "Acme", name: "blake", companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());

            // non analyzed field
            var results = await _employeeRepository.GetNumberOfEmployeesWithMissingCompanyName(employee1.CompanyId);
            Assert.Equal(1, results.Total);

            // analyzed field
            results = await _employeeRepository.GetNumberOfEmployeesWithMissingName(employee1.CompanyId);
            Assert.Equal(1, results.Total);
        }
    }
}