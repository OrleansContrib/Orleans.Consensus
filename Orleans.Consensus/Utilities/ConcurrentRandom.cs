namespace Orleans.Consensus.Utilities
{
    using System;
    using System.Security.Cryptography;
    using Orleans.Consensus.Contract;

    public class ConcurrentRandom : IRandom
    {
        private static readonly RNGCryptoServiceProvider GlobalRandom = new RNGCryptoServiceProvider();

        [ThreadStatic]
        private static Random local;

        public static ConcurrentRandom Instance { get; } = new ConcurrentRandom();

        public int Next(int minValue, int maxValue)
        {
            var inst = local;
            if (inst == null)
            {
                var buffer = new byte[4];
                GlobalRandom.GetBytes(buffer);
                local = inst = new Random(BitConverter.ToInt32(buffer, 0));
            }

            return inst.Next(minValue, maxValue);
        }
    }
}