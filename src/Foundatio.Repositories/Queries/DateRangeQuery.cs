using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    [DebuggerDisplay("{Field}: {StartDate} - {EndDate}")]
    public class DateRange {
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public QueryField Field { get; set; }

        public bool UseStartDate => StartDate.HasValue && StartDate.Value > DateTime.MinValue;

        public bool UseEndDate => EndDate.HasValue;

        public bool UseDateRange => Field != null && (UseStartDate || UseEndDate);

        public DateTime GetStartDate() {
            return UseStartDate ? StartDate.GetValueOrDefault() : DateTime.MinValue;
        }

        public DateTime GetEndDate() {
            return UseEndDate ? EndDate.GetValueOrDefault() : SystemClock.UtcNow.AddHours(1);
        }
    }

    public static class DateRangesQueryExtensions {
        internal const string DateRangesKey = "@DateRanges";

        public static T DateRange<T>(this T query, DateTime? utcStart, DateTime? utcEnd, QueryField field) where T : IRepositoryQuery {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            return query.AddCollectionOptionValue(DateRangesKey, new DateRange { StartDate = utcStart, EndDate = utcEnd, Field = field });
        }

        public static T DateRange<T, TModel>(this T query, DateTime? utcStart, DateTime? utcEnd, Expression<Func<TModel, object>> objectPath) where T : IRepositoryQuery {
            if (objectPath == null)
                throw new ArgumentNullException(nameof(objectPath));

            return query.AddCollectionOptionValue(DateRangesKey, new DateRange { StartDate = utcStart, EndDate = utcEnd, Field = objectPath });
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadDateRangesQueryExtensions {
        public static ICollection<DateRange> GetDateRanges(this IRepositoryQuery query) {
            return query.SafeGetCollection<DateRange>(DateRangesQueryExtensions.DateRangesKey);
        }
    }
}
