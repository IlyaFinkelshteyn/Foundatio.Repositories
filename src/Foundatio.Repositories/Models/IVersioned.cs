using System;

namespace Foundatio.Repositories.Models {
    public interface IVersioned {
        /// <summary>
        /// Current modification version for the document.
        /// </summary>
        object Version { get; set; }
    }

    public static class VersionedExtensions {
        public static Guid GetVersionAsGuidOrDefault(this IVersioned versioned) {
            var version = ToGuid(versioned?.Version);
            return version ?? Guid.Empty;
        }

        public static Guid GetVersionAsGuidOrDefault<T>(this FindHit<T> versioned) {
            var version = ToGuid(versioned?.Version);
            return version ?? Guid.Empty;
        }

        public static Guid? GetVersionAsGuid(this IVersioned versioned) {
            return ToGuid(versioned?.Version);
        }

        public static Guid? GetVersionAsGuid<T>(this FindHit<T> versioned) {
            return ToGuid(versioned?.Version);
        }

        private static Guid? ToGuid(object version) {
            if (version == null)
                return null;

            if (version is Guid)
                return (Guid)version;

            var s = version as string;
            if (s != null)
                return Guid.Parse(s);

            return null;
        }

        public static long GetVersionAsLongOrDefault(this IVersioned versioned) {
            var version = ToLong(versioned?.Version);
            return version ?? 0;
        }

        public static long GetVersionAsLongOrDefault<T>(this FindHit<T> versioned) {
            var version = ToLong(versioned?.Version);
            return version ?? 0;
        }

        public static long? GetVersionAsLong(this IVersioned versioned) {
            return ToLong(versioned?.Version);
        }

        public static long? GetVersionAsLong<T>(this FindHit<T> versioned) {
            return ToLong(versioned?.Version);
        }

        private static long? ToLong(object version) {
            if (version == null)
                return null;

            if (version is long)
                return (long)version;

            var s = version as string;
            if (s != null)
                return Int32.Parse(s);

            return null;
        }
    }
}
