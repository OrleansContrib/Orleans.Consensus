namespace Orleans.Consensus.State
{
    using System;
    using System.Threading.Tasks;

    public class OrleansStorageRaftPersistentState<TOperation> : IRaftPersistentState
    {
        private readonly RaftGrainState<TOperation> state;

        public OrleansStorageRaftPersistentState(RaftGrainState<TOperation> state, Func<Task> writeStateFunc)
        {
            this.state = state;
            this.WriteState = writeStateFunc;
        }

        private Func<Task> WriteState { get; set; }

        public string VotedFor => this.state.VotedFor;

        public long CurrentTerm => this.state.CurrentTerm;

        public Task UpdateTermAndVote(string votedFor, long currentTerm)
        {
            this.state.VotedFor = votedFor;
            this.state.CurrentTerm = currentTerm;
            return this.WriteState();
        }
    }
}