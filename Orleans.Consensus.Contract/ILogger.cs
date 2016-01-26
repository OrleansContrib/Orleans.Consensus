namespace Orleans.Consensus.Contract
{
    public interface ILogger
    {
        void LogInfo(string message);

        void LogWarn(string message);

        void LogVerbose(string message);
    }
}