namespace Orleans.Consensus.Contract.Messages
{
    using System;

    using Orleans.Concurrency;

    [Immutable]
    [Serializable]
    public class NotLeaderException : Exception
    {
        public NotLeaderException(string leader)
            : base($"This instance is not the leader, '{leader ?? "no instance"}' is.")
        {
            this.Leader = leader;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:System.Exception"/> class.
        /// </summary>
        public NotLeaderException() {}

        /// <summary>
        /// Initializes a new instance of the <see cref="T:System.Exception"/> class with a specified error message and a reference to the inner exception that is the cause of this exception.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception. </param><param name="innerException">The exception that is the cause of the current exception, or a null reference (Nothing in Visual Basic) if no inner exception is specified. </param>
        public NotLeaderException(string message, Exception innerException)
            : base(message, innerException) {}
        
        public string Leader { get; set; }
    }
}