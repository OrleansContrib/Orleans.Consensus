using Orleans;
using Orleans.Providers;
using System;
using System.Threading.Tasks;

namespace OrleansRaft
{
    using OrleansRaft.Actors;
    using OrleansRaft.Messages;

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
            Task.Factory.StartNew(
                async () =>
                {
                    var num = 0;
                    var log = providerRuntime.GetLogger("Bootstrap");
                    var leader = "one";
                    var grain = providerRuntime.GrainFactory.GetGrain<ITestRaftGrain>(leader);
                    grain.AddValue(null).Ignore();
                    await Task.Delay(TimeSpan.FromSeconds(4));
                    while (true)
                    {
                        try
                        {
                            grain = providerRuntime.GrainFactory.GetGrain<ITestRaftGrain>(leader);
                            log.Info("Trying to replicate a log entry...");
                            await grain.AddValue($"please agree {num}");
                            num++;
                        }
                        catch (NotLeaderException exception)
                        {
                            leader = exception.Leader;
                            log.Warn(-1, $"Exception replicating value: {exception}, leader is {leader}");
                        }
                        catch (Exception exception)
                        {
                            log.Info(-1, $"Exception replicating value: {exception}");
                        }

                        await Task.Delay(TimeSpan.FromSeconds(4));
                    }
                });
            return Task.FromResult(0);
        }
    }
}
