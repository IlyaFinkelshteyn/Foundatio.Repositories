using System;
using System.Linq.Expressions;
using Baseline.Reflection;
using Foundatio.Repositories.Queries;
using Marten.Schema;

namespace Foundatio.Repositories.Marten.Extensions {
    public static class DocumentMappingExtensions {
        public static IField FieldFor(this DocumentMapping mapping, QueryField field) {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            if (field.Expression != null)
                return mapping.FieldFor(ReflectionHelper.GetProperty(field.Expression as LambdaExpression));

            if (field.Property != null)
                return mapping.FieldFor(field.Property);

            return mapping.FieldFor(field.Name);
        }
    }
}
