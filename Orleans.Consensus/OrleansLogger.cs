namespace Orleans.Consensus.Actors
{
    using System;

    using Orleans.Runtime;

    public class OrleansLogger : ILogger
    {
        private readonly Logger log;

        public OrleansLogger(Logger log)
        {
            this.log = log;
        }

        public Func<string, string> FormatMessage { get; set; } = _ => _;

        public void LogInfo(string message)
        {
            this.log.Info(this.FormatMessage(message));
        }

        public void LogWarn(string message)
        {
            this.log.Warn(message.GetHashCode(), this.FormatMessage(message));
        }

        public void LogVerbose(string message)
        {
            this.log.Verbose(this.FormatMessage(message));
        }
    }
}