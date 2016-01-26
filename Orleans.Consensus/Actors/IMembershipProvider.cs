namespace Orleans.Consensus.Actors
{
    using System.Collections.Generic;

    public interface IMembershipProvider
    {
        IReadOnlyCollection<string> AllServers { get; }
        IReadOnlyCollection<string> OtherServers { get; }
    }
}