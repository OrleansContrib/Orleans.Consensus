namespace Orleans.Consensus.Actors
{
    public interface IRandom
    {
        int Next(int minValue, int maxValue);
    }
}