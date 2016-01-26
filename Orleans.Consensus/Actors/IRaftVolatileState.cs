namespace Orleans.Consensus.Actors
{
    public interface IRaftVolatileState
    {
        long CommitIndex { get; set; }
        long LastApplied { get; set; }
        string LeaderId { get; set; }
    }
}