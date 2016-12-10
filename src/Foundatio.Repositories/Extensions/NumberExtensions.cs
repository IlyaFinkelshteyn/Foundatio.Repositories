﻿using System;

namespace Foundatio.Repositories.Extensions {
    public static class NumericExtensions {
        public static string ToOrdinal(this int num) {
            switch (num % 100) {
                case 11:
                case 12:
                case 13:
                    return num.ToString("#,###0") + "th";
            }

            switch (num % 10) {
                case 1:
                    return num.ToString("#,###0") + "st";
                case 2:
                    return num.ToString("#,###0") + "nd";
                case 3:
                    return num.ToString("#,###0") + "rd";
                default:
                    return num.ToString("#,###0") + "th";
            }
        }

        private static DateTime _epoch = new DateTime(1970, 1, 1);

        public static DateTime FromUnixTime(this double value) {
            return DateTime.SpecifyKind(_epoch.AddMilliseconds(0 + value), DateTimeKind.Unspecified);
        }
    }
}