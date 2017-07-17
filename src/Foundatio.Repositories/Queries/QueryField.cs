using System;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

namespace Foundatio.Repositories.Queries {
    [DebuggerDisplay("{" + nameof(DebugDisplay) + ",nq}")]
    public class QueryField {
        public QueryField(string name) {
            if (String.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));
            Name = name;
        }

        public QueryField(Expression expression) {
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        public QueryField(PropertyInfo property) {
            Property = property ?? throw new ArgumentNullException(nameof(property));
        }

        public string Name { get; }
        public Expression Expression { get; }
        public PropertyInfo Property { get; }

        internal string DebugDisplay =>
            $"{Expression?.ToString() ?? PropertyDebug ?? Name}";

        private string PropertyDebug => Property == null ? null : $"PropertyInfo: {Property.Name}";

        public static implicit operator QueryField(string name) {
            return String.IsNullOrEmpty(name) ? null : new QueryField(name);
        }

        public static implicit operator QueryField(Expression expression) {
            return expression == null ? null : new QueryField(expression);
        }

        public static implicit operator QueryField(PropertyInfo property) {
            return property == null ? null : new QueryField(property);
        }
    }

    public class QueryFieldSort {
        public QueryField Field { get; set; }
        public SortOrder? Order { get; set; }
    }

    public enum SortOrder {
        Ascending,
        Descending
    }
}