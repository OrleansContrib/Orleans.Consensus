namespace Orleans.Consensus.UnitTests
{
    using FluentAssertions;

    using Orleans.Consensus.Contract.Log;

    using Xunit;

    public class LogEntryIdTests
    {
        /// <summary>
        /// Tests equality between log ids.
        /// </summary>
        [Fact]
        public void ComparisonTest()
        {
            var log = new[]
            {
                default(LogEntryId),
                new LogEntryId(1, 2),
                new LogEntryId(1, 3),
                new LogEntryId(1, 4),
                new LogEntryId(2, 2),
                new LogEntryId(2, 3),
                new LogEntryId(2, 4)
            };

            for (var i = 0; i < log.Length; i++)
            {
                for (var j = 0; j < log.Length; j++)
                {
                    if (i < j)
                    {
                        log[i].Should().BeLessThan(log[j]);
                        log[i].Should().BeLessOrEqualTo(log[j]);
                        log[i].CompareTo(log[j]).Should().Be(-1);
                    }
                    else if (i == j)
                    {
                        log[i].Should().BeLessOrEqualTo(log[j]);
                        log[i].Should().Be(log[j]);
                        log[i].Should().BeGreaterOrEqualTo(log[j]);
                        log[i].CompareTo(log[j]).Should().Be(0);
                    }
                    else
                    {
                        log[i].Should().BeGreaterThan(log[j]);
                        log[i].Should().BeGreaterOrEqualTo(log[j]);
                        log[i].CompareTo(log[j]).Should().Be(1);
                    }
                }
            }
        }
    }
}
