using System.Threading.Tasks;

namespace Orleans.Consensus
{
    using System.Threading;

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
