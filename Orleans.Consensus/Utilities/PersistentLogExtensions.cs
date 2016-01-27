namespace Orleans.Consensus.Utilities
{
    using System.Linq;

    using Orleans.Consensus.Contract.Log;

    public static class PersistentLogExtensions
    {
        public static string ProgressString<TOperation>(this IPersistentLog<TOperation> log)
        {
            return $"[{string.Join(", ", log.GetCursor(0).Select(_ => _.Id))}]";
        }
    }
}