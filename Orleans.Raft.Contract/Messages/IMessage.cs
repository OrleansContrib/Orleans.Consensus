namespace Orleans.Raft.Contract.Messages
{
    public interface IMessage
    {
        long Term { get; }
    }
}