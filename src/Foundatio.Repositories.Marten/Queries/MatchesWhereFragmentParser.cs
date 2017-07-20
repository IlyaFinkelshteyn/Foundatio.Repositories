using System.Linq.Expressions;
using Marten;
using Marten.Linq;
using Marten.Linq.Parsing;
using Marten.Schema;

namespace Foundatio.Repositories.Marten.Queries {
    public class MatchesWhereFragmentParser : IMethodCallParser {
        public bool Matches(MethodCallExpression expression) {
            return expression.Method.Name == nameof(MatchesWhereFragmentParserExtensions.MatchesWhereFragment);
        }

        public IWhereFragment Parse(IQueryableDocument mapping, ISerializer serializer, MethodCallExpression expression) {
            if (expression.Method.GetParameters().Length == 3)
                return new WhereFragment(expression.Arguments[1].Value() as string, expression.Arguments[2].Value() as object[]);

            return expression.Arguments[1].Value() as IWhereFragment;
        }
    }

    public static class MatchesWhereFragmentParserExtensions {
        public static bool MatchesWhereFragment(this object target, IWhereFragment whereFragment) {
            return true;
        }

        public static bool MatchesWhereFragment(this object target, string sql, params object[] parameters) {
            return true;
        }
    }
}
