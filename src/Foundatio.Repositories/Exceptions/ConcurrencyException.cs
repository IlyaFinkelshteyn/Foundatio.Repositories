using System;

namespace Foundatio.Repositories.Exceptions {
    public class ConcurrencyException : Exception {
        public ConcurrencyException(string message, Exception innerException) : base(message, innerException) {}
    }
}