using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace TestConsole
{
    using System.Net;

    using Orleans;
    using Orleans.Raft.Contract;
    using Orleans.Raft.Contract.Messages;
    using Orleans.Runtime;
    using Orleans.Runtime.Configuration;
    using Orleans.Storage;

    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
            var config = GetClusterConfiguration();

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

        private string leader = "one";
        private async void Client_AppendText(object sender, RoutedEventArgs e)
        {
            try
            {
                var grain = GrainClient.GrainFactory.GetGrain<ITestRaftGrain>(leader);
                await grain.AddValue(this.AppendText.Text);
            }
            catch (NotLeaderException exception)
            {
                if (!string.IsNullOrWhiteSpace(exception.Leader))
                {
                    this.leader = exception.Leader;
                }
            }catch { }
        }
    }
}
