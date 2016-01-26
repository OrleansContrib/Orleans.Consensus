namespace Orleans.Consensus.Actors
{
    using System;
    using System.Text;
    using System.Threading.Tasks;

    using Autofac;

    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Log;
    using Orleans.Runtime;

    public class TestRaftGrain : RaftGrain<string>, ITestRaftGrain
    {
        private readonly BigString stateMachine;

        private Logger log;

        public TestRaftGrain()
        {
            this.stateMachine = new BigString();
        }

        public Task AddValue(string value)
        {
            this.log.Error(0, $"TEST: AddValue({value})");
            if (string.IsNullOrEmpty(value))
            {
                return Task.FromResult(0);
            }
            return this.AppendEntry(value);
        }

        public Task Crash()
        {
            this.log.Error(0, "TEST: Crash()");
            this.DeactivateOnIdle();
            return Task.FromResult(0);
        }

        public async Task Delay(TimeSpan delay)
        {
            this.log.Error(0, "TEST: Stall");
            await Task.Delay(delay);
            this.log.Error(0, "TEST: Stopped stalling");
        }

        public Task<string> GetState()
        {
            return Task.FromResult(this.stateMachine.GetValue());
        }

        protected override IStateMachine<string> GetStateMachine(IComponentContext context) => this.stateMachine;

        /// <summary>
        /// This method is called at the end of the process of activating a grain.
        ///             It is called before any messages have been dispatched to the grain.
        ///             For grains with declared persistent state, this method is called after the State property has been populated.
        /// </summary>
        public override async Task OnActivateAsync()
        {
            this.log = this.GetLogger($"TEST {this.GetPrimaryKeyString()}");
            await base.OnActivateAsync();
        }
    }

    public class BigString : IStateMachine<string>
    {
        private readonly StringBuilder builder = new StringBuilder();

        private LogEntryId previousEntryId;

        public Task Reset()
        {
            this.previousEntryId = default(LogEntryId);
            this.builder.Clear();

            return Task.FromResult(0);
        }

        public Task Apply(LogEntry<string> entry)
        {
            if (entry.Id.Index != this.previousEntryId.Index + 1)
            {
                throw new InvalidOperationException(
                    $"Tried to apply Entry({entry.Id}) which is not subsequent to previous entry ({this.previousEntryId})");
            }

            this.previousEntryId = entry.Id;
            this.builder.Append(entry.Operation);
            return Task.FromResult(0);
        }

        public string GetValue()
        {
            return this.builder.ToString();
        }
    }
}