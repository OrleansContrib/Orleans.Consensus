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

            Console.WriteLine("Silo configuration: \n" + silo.Config.ToString(name));

            silo.InitializeOrleansSilo();
            Console.WriteLine("Successfully initialized Orleans silo '{0}' as a {1} node.", silo.Name, silo.Type);
            Console.WriteLine("Starting Orleans silo '{0}' as a {1} node.", silo.Name, silo.Type);

            if (silo.StartOrleansSilo())
            {
                silo.WaitForOrleansSiloShutdown();
            }
        }

        public static ClusterConfiguration GetClusterConfiguration()
        {
            var config = new ClusterConfiguration();

            // Configure logging and metrics collection.
            config.Defaults.StatisticsCollectionLevel = StatisticsLevel.Info;
            config.Defaults.StatisticsLogWriteInterval = TimeSpan.FromDays(6);
            config.Defaults.TurnWarningLengthThreshold = TimeSpan.FromSeconds(15);

            // Configure providers
            config.Globals.RegisterBootstrapProvider<RaftBootstrap>(new RaftBootstrap().Name);
            config.Globals.RegisterStorageProvider<MemoryStorage>("Default");
            config.Globals.ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.ReminderTableGrain;
            config.Globals.LivenessType = GlobalConfiguration.LivenessProviderType.MembershipTableGrain;

            // Configure clustering.
            config.Globals.ClusterId = "test";
            //config.Globals.ExpectedClusterSize = nodeList.Count; // An overestimate is tolerable.
            config.Globals.ResponseTimeout = TimeSpan.FromSeconds(90);

            return config;
        }
    }
}