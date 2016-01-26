namespace TestConsole
{
    using System;
    using System.Net;
    using System.Windows;

    using Orleans;
    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Messages;
    using Orleans.Runtime.Configuration;

    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            this.InitializeComponent();
            var config = GetClusterConfiguration();

            this.Leader.Text = "one";
            while (true)
            {
                try
                {
                    GrainClient.Initialize(config);
                    break;
                }
                catch {}
            }
        }

        private async void One_Stall(object sender, RoutedEventArgs e)
        {
            var grain = GrainClient.GrainFactory.GetGrain<ITestRaftGrain>("one");
            await grain.Delay(TimeSpan.FromSeconds(15));
        }

        private async void One_Crash(object sender, RoutedEventArgs e)
        {
            var grain = GrainClient.GrainFactory.GetGrain<ITestRaftGrain>("one");
            await grain.Crash();
        }

        private async void Two_Stall(object sender, RoutedEventArgs e)
        {
            var grain = GrainClient.GrainFactory.GetGrain<ITestRaftGrain>("two");
            await grain.Delay(TimeSpan.FromSeconds(15));
        }

        private async void Two_Crash(object sender, RoutedEventArgs e)
        {
            var grain = GrainClient.GrainFactory.GetGrain<ITestRaftGrain>("two");
            await grain.Crash();
        }

        private async void Three_Stall(object sender, RoutedEventArgs e)
        {
            var grain = GrainClient.GrainFactory.GetGrain<ITestRaftGrain>("three");
            await grain.Delay(TimeSpan.FromSeconds(15));
        }

        private async void Three_Crash(object sender, RoutedEventArgs e)
        {
            var grain = GrainClient.GrainFactory.GetGrain<ITestRaftGrain>("three");
            await grain.Crash();
        }

        public static ClientConfiguration GetClusterConfiguration()
        {
            var config = new ClientConfiguration();

            // Configure logging and metrics collection.
            config.Gateways.Add(new IPEndPoint(IPAddress.Loopback, 12345));
            config.TraceToConsole = true;
            // Configure clustering.
            config.DeploymentId = "test";
            //config.Globals.ExpectedClusterSize = nodeList.Count; // An overestimate is tolerable.
            config.ResponseTimeout = TimeSpan.FromSeconds(90);

            return config;
        }

        private async void Client_AppendText(object sender, RoutedEventArgs e)
        {
            NotLeaderException notLeaderException = null;
            try
            {
                var grain = GrainClient.GrainFactory.GetGrain<ITestRaftGrain>(this.Leader.Text);
                await grain.AddValue(this.AppendText.Text);
            }
            catch (AggregateException aggregateException)
            {
                aggregateException.Flatten();
                notLeaderException = aggregateException.InnerException as NotLeaderException;
            }
            catch (NotLeaderException exception)
            {
                notLeaderException = exception;
            }
            catch {}

            if (notLeaderException != null)
            {
                if (!string.IsNullOrWhiteSpace(notLeaderException.Leader))
                {
                    this.Leader.Text = notLeaderException.Leader;
                }
            }
        }
    }
}