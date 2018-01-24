namespace Orleans.Consensus
{
    using System.Threading.Tasks;

    using Orleans.Consensus.Contract;
    using Orleans.Providers;

    public class RaftBootstrap : IBootstrapProvider
    {
        public string Name => "Raft Bootstrap Provider";

        public Task Close()
        {
            return TaskDone.Done;
        }

        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            foreach (var server in new[] { "one", "two", "three" })
            {
                providerRuntime.GrainFactory.GetGrain<ITestRaftGrain>(server).AddValue(null);
            }
#if false
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
                            await Task.Delay(TimeSpan.FromSeconds(2));
                        }
                        catch (NotLeaderException exception)
                        {
                            leader = exception.Leader;
                            log.Warn(-1, $"Exception replicating value: {exception}, leader is {leader}");
                            await Task.Delay(TimeSpan.FromSeconds(4));
                        }
                        catch (Exception exception)
                        {
                            log.Info(-1, $"Exception replicating value: {exception}");
                            await Task.Delay(TimeSpan.FromSeconds(4));
                        }
                    }
                });
#endif
            return Task.FromResult(0);
        }
    }
}