namespace Orleans.Consensus.Contract.Messages
{
    public interface IMessage
    {
        long Term { get; }
    }
}