using System;

namespace Foundatio.Repositories.Exceptions {
    public class DuplicateDocumentException : Exception {
        public DuplicateDocumentException(string message, Exception innerException) : base(message, innerException) {}
    }
}