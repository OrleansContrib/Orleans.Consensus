namespace Orleans.Consensus.Roles
{
    public interface ILeaderRole<TOperation> : IRaftRole<TOperation> { }
}