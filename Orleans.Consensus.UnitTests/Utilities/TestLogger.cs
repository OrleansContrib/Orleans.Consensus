namespace Orleans.Consensus.UnitTests.Utilities
{
    using Orleans.Consensus.Contract;

    using Xunit.Abstractions;

    internal class TestLogger : ILogger
    {
        private readonly ITestOutputHelper output;

        public TestLogger(ITestOutputHelper output)
        {
            this.output = output;
        }

        public void LogInfo(string message)
        {
            this.output.WriteLine(message);
        }

        public void LogWarn(string message)
        {
            this.output.WriteLine($"[WARNING] {message}");
        }

        public void LogVerbose(string message)
        {
            this.output.WriteLine($"[ERROR] {message}");
        }
    }
}
