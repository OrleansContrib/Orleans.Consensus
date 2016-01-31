namespace Orleans.Consensus.Utilities
{
    using System.Threading;
    using System.Threading.Tasks;

    internal static class CancellationTokenExtensions
    {
        public static Task<T> WhenCanceled<T>(this CancellationToken token)
        {
            var completion = new TaskCompletionSource<T>();
            token.Register(completion.SetCanceled);
            return completion.Task;
        }
    }
}