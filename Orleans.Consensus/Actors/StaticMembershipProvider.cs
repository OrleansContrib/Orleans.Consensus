namespace Orleans.Consensus.Actors
{
    using System.Collections.Generic;

    public class StaticMembershipProvider : IMembershipProvider
    {
        private readonly IServerIdentity identity;

        private List<string> otherServers;

        public StaticMembershipProvider(IServerIdentity identity)
        {
            this.identity = identity;
        }

        public IReadOnlyCollection<string> AllServers { get; private set; }

        public IReadOnlyCollection<string> OtherServers => this.otherServers;

        public void SetServers(IReadOnlyCollection<string> servers)
        {
            this.AllServers = servers;
            this.otherServers = new List<string>(this.AllServers);
            this.otherServers.Remove(this.identity.Id);
        }
    }
}