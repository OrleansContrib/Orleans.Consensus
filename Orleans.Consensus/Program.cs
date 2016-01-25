namespace Orleans.Consensus
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Net;

    using Orleans.Runtime;
    using Orleans.Runtime.Configuration;
    using Orleans.Runtime.Host;
    using Orleans.Storage;

    public class Program
    {
        public static void Main(string[] args)
        {
            while (true)
            {
                try
                {
                    Run();
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"FATAL Exception:\n{exception}");
                }

                Console.WriteLine("Press Esc to exit, or any other key to restart.");
                var key = Console.ReadKey();
                if (key.Key == ConsoleKey.Escape)
                {
                    break;
                }
            }
        }
        private static void Run()
        {
            var config = GetClusterConfiguration();
            config.Globals.SeedNodes.Add(new IPEndPoint(IPAddress.Loopback, 11111));
            config.Defaults.HostNameOrIPAddress = "localhost";
            config.Defaults.Port = 11111;
            config.Defaults.ProxyGatewayEndpoint = new IPEndPoint(IPAddress.Loopback, 12345);

            var process = Process.GetCurrentProcess();
            var name = Environment.MachineName + "_" + process.Id + Guid.NewGuid().ToString("N").Substring(3);

            var silo = new SiloHost(name, config);

            // Configure the silo for the current environment.
            silo.SetSiloType(Silo.SiloType.Primary);

            Trace.TraceInformation("Silo configuration: \n" + silo.Config.ToString(name));

            silo.InitializeOrleansSilo();
            Trace.TraceInformation(
                "Successfully initialized Orleans silo '{0}' as a {1} node.",
                silo.Name,
                silo.Type);
            Trace.TraceInformation("Starting Orleans silo '{0}' as a {1} node.", silo.Name, silo.Type);

            if (silo.StartOrleansSilo())
            {
                silo.WaitForOrleansSiloShutdown();
            }
        }

        public static ClusterConfiguration GetClusterConfiguration()
        {
            var config = new ClusterConfiguration();

            // Configure logging and metrics collection.
            config.Defaults.TraceFilePattern = Path.GetTempPath() + "{0}_{1}.log";
            config.Defaults.StatisticsCollectionLevel = StatisticsLevel.Info;
            config.Defaults.StatisticsLogWriteInterval = TimeSpan.FromDays(6);
            config.Defaults.TurnWarningLengthThreshold = TimeSpan.FromSeconds(15);
            config.Defaults.TraceToConsole = true;
            config.Defaults.WriteMessagingTraces = false;
            config.Defaults.DefaultTraceLevel = Severity.Info;
            /*config.Defaults.TraceLevelOverrides.Add(Tuple.Create("Orleans", Severity.Warning));
            config.Defaults.TraceLevelOverrides.Add(Tuple.Create("Runtime", Severity.Warning));
            config.Defaults.TraceLevelOverrides.Add(Tuple.Create("Dispatcher", Severity.Warning));
            config.Defaults.TraceLevelOverrides.Add(Tuple.Create("MembershipOracle", Severity.Warning));
            config.Defaults.TraceLevelOverrides.Add(Tuple.Create("DeploymentLoadPublisher", Severity.Warning));
            config.Defaults.TraceLevelOverrides.Add(Tuple.Create("ReminderService", Severity.Warning));
            config.Defaults.TraceLevelOverrides.Add(Tuple.Create("SiloLogStatistics", Severity.Warning));*/
            config.Defaults.TraceLevelOverrides.Add(Tuple.Create("Catalog", Severity.Warning));
            config.Defaults.TraceLevelOverrides.Add(Tuple.Create("sync.SafeTimerBase", Severity.Warning));
            config.Defaults.TraceLevelOverrides.Add(Tuple.Create("asynTask.SafeTimerBase", Severity.Warning));

            // Configure providers
            config.Globals.RegisterBootstrapProvider<RaftBootstrap>(new RaftBootstrap().Name);
            config.Globals.RegisterStorageProvider<MemoryStorage>("Default");
            config.Globals.ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.ReminderTableGrain;
            config.Globals.LivenessType = GlobalConfiguration.LivenessProviderType.MembershipTableGrain;

            // Configure clustering.
            config.Globals.DeploymentId = "test";
            //config.Globals.ExpectedClusterSize = nodeList.Count; // An overestimate is tolerable.
            config.Globals.ResponseTimeout = TimeSpan.FromSeconds(90);

            return config;
        }
    }
}
