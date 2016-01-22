using Orleans;
using Orleans.Providers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansRaft
{
    public class RaftBootstrap : IBootstrapProvider
    {
        public string Name
        {
            get
            {
                return "Raft Bootstrap Provider";
            }
        }

        public Task Close()
        {
            return TaskDone.Done;
        }

        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            var grain = providerRuntime.GrainFactory.GetGrain<IRaftGrain>(providerRuntime.SiloIdentity);
            return grain.Append(null);
        }
    }
}
