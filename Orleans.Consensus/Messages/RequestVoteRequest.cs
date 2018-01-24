namespace Orleans.Consensus.Contract.Messages
{
    using System;

    using Orleans.Concurrency;
    using Orleans.Consensus.Contract.Log;

    [Immutable]
    [Serializable]
    public struct RequestVoteRequest : IMessage
    {
        public RequestVoteRequest(long term, string candidate, LogEntryId lastLogEntryId)
        {
            this.Term = term;
            this.Candidate = candidate;
            this.LastLogEntryId = lastLogEntryId;
        }

        public long Term { get; }
        public string Candidate { get; }
        public LogEntryId LastLogEntryId { get; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return
                $"{nameof(RequestVoteRequest)}(Candidate: {this.Candidate}, LastLogEntryId: {this.LastLogEntryId}, Term: {this.Term})";
        }
    }
}