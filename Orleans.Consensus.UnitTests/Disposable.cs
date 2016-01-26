namespace Orleans.Consensus.UnitTests
{
    using System;

    public class Disposable : IDisposable
    {
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this.Disposed = true;
        }

        public bool Disposed { get; set; }
    }
}