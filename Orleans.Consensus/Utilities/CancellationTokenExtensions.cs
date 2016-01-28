namespace Orleans.Consensus.Utilities
{
    using System.Threading;
    using System.Threading.Tasks;

    internal static class CancellationTokenExtensions
    {
        public static Task WhenCanceled(this CancellationToken token)
        {
            var completion = new TaskCompletionSource<int>();
            token.Register(completion.SetCanceled);
            return completion.Task;
        }
    }
}