namespace Orleans.Consensus.Contract
{
    public interface IRandom
    {
        int Next(int minValue, int maxValue);
    }
}