namespace OrleansRaft.Messages
{
    using System;

    [Serializable]
    public class AppendResponse : IMessage
    {
        public long Term { get; set; }
        public bool Success { get; set; }
    }
}