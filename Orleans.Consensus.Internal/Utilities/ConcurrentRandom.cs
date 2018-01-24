namespace Orleans.Consensus.Utilities
{
    using System;
    using Orleans.Consensus.Contract;

    public class ConcurrentRandom : IRandom
    {
        [ThreadStatic]
        private static Random local;

        public static ConcurrentRandom Instance { get; } = new ConcurrentRandom();

        public int Next(int minValue, int maxValue)
        {
            var inst = local;
            if (inst == null)
            {
                local = inst = new Random(Guid.NewGuid().GetHashCode());
            }

            return inst.Next(minValue, maxValue);
        }
    }
}