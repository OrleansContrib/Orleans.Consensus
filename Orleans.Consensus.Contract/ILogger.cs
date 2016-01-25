namespace Orleans.Consensus.Actors
{
    public interface ILogger {
        void LogInfo(string message);

        void LogWarn(string message);

        void LogVerbose(string message);
    }
}