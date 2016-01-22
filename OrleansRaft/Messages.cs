using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansRaft
{
    public enum NodeState
    {
        Follower,
        Candidate,
        Leader
    }

    [Serializable]
    public class AppendRequest
    {
        public int Term { get; set; }
        public string Leader { get; set; }
        public int PreviousLogIndex { get; set; }
        public int PreviousLogTerm { get; set; }
        /// <summary>
        /// Empty for heartbeat
        /// </summary>
        //public LogEntry<T>[] Entries { get; set; }
        public int LeaderCommitIndex { get; set; }
    }

    [Serializable]
    public class AppendResponse
    {
        public int Term { get; set; }
        public bool Success { get; set; }
    }

    [Serializable]
    public class RequestVoteRequest
    {
        public int Term { get; set; }
        public string Candidate { get; set; }
        public int LastLogIndex { get; set; }
        public int LastLogTerm { get; set; }
    }

    [Serializable]
    public class RequestVoteResponse
    {
        public int Term { get; set; }
        public bool VoteGranted { get; set; }
    }
}
