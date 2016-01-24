namespace OrleansRaft.Messages
{
    public interface IMessage
    {
        long Term { get; }
    }
}