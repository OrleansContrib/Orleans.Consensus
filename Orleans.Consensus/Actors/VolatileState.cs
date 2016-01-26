namespace Orleans.Consensus.Actors
{
    public class VolatileState : IRaftVolatileState
    {
        public long CommitIndex { get; set; }
        public long LastApplied { get; set; }
        public string LeaderId { get; set; }
    }
}