namespace Orleans.Consensus.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using AutofacContrib.NSubstitute;

    using FluentAssertions;

    using NSubstitute;

    using Orleans.Consensus.Actors;
    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Contract.Messages;
    using Orleans.Consensus.Log;
    using Orleans.Consensus.Roles;
    using Orleans.Consensus.State;
    using Orleans.Consensus.UnitTests.Utilities;

    using Xunit;
    using Xunit.Abstractions;

    public class FollowerRoleTests
    {
        private const int MinElectionTime = 72;

        private const int MaxElectionTime = 720;

        private const int RiggedRandomResult = 440;

        private readonly IRoleCoordinator<int> coordinator;

        private readonly InMemoryLog<int> journal;

        private readonly IRaftPersistentState persistentState;

        private readonly IRandom random;

        private readonly FollowerRole<int> role;

        private readonly ISettings settings;

        private readonly IStateMachine<int> stateMachine;

        private readonly MockTimers timers;

        public FollowerRoleTests(ITestOutputHelper output)
        {
            var builder = new AutoSubstitute();
            builder.Provide<ILogger>(new TestLogger(output));

            // Configure settings
            this.settings = builder.Resolve<ISettings>();
            this.settings.ApplyEntriesOnFollowers.Returns(true);
            this.settings.MinElectionTimeoutMilliseconds.Returns(MinElectionTime);
            this.settings.MaxElectionTimeoutMilliseconds.Returns(MaxElectionTime);

            // Rig random number generator to always return the same value.
            this.random = builder.Resolve<IRandom>();
            this.random.Next(Arg.Any<int>(), Arg.Any<int>()).Returns(RiggedRandomResult);

            this.coordinator = builder.Resolve<IRoleCoordinator<int>>();
            this.stateMachine = builder.Resolve<IStateMachine<int>>();

            this.timers = new MockTimers();
            builder.Provide<RegisterTimerDelegate>(this.timers.RegisterTimer);

            this.persistentState = builder.Resolve<IRaftPersistentState>();
            this.journal = Substitute.ForPartsOf<InMemoryLog<int>>();
            builder.Provide<IPersistentLog<int>>(this.journal);

            // After the container is configured, resolve required services.
            this.role = builder.Resolve<FollowerRole<int>>();
        }

        [Fact]
        public async Task EntryStartsElectionTimer()
        {
            await this.role.Enter();

            // The state machine should have been reset.
            await this.stateMachine.Received().Reset();

            // A timer should have been registered.
            this.timers.Registrations.Should().HaveCount(1);

            // Check that the correct timer was registered.
            var timer = this.timers[0];
            this.random.Received()
                .Next(this.settings.MinElectionTimeoutMilliseconds, this.settings.MaxElectionTimeoutMilliseconds);
            timer.DueTime.Should().Be(TimeSpan.FromMilliseconds(RiggedRandomResult));
            timer.Period.Should().Be(TimeSpan.FromMilliseconds(RiggedRandomResult));
            timer.Disposable.Disposed.Should().BeFalse();

            // Check that the timer causes the instance to become a candidate.
            await this.coordinator.DidNotReceive().BecomeCandidate();
            await this.timers[0].Callback(null);
            await this.coordinator.Received().BecomeCandidate();
        }

        [Fact]
        public async Task ExitDisposesTimer()
        {
            await this.EntryStartsElectionTimer();
            await this.role.Exit();

            this.timers[0].Disposable.Disposed.Should().BeTrue();
        }

        [Fact]
        public async Task BecomeCandidateWhenNotMessaged()
        {
            // Become a follower
            await this.role.Enter();

            // Receive a valid append message.
            this.persistentState.CurrentTerm.Returns(_ => 1);
            var response = await this.role.Append(new AppendRequest<int> { Term = 1 });
            response.Success.Should().BeTrue();

            // Fire the election timer and ensure no transition to candidate occurred.
            await this.timers[0].Callback(null);
            await this.coordinator.DidNotReceive().BecomeCandidate();

            // Fire the election timer again, without the instance having received a call between firings.
            // Ensure a transition to candidate occurred.
            await this.timers[0].Callback(null);
            await this.coordinator.Received().BecomeCandidate();
        }

        [Fact]
        public async Task ValidEntriesAreWrittenToLog()
        {
            this.persistentState.CurrentTerm.Returns(_ => 1);
            this.journal.Entries.Add(new LogEntry<int>(new LogEntryId(1, 1), 27));

            // Become a follower
            await this.role.Enter();

            // Append some entries.
            var expectedEntries = new List<LogEntry<int>>
            {
                new LogEntry<int>(new LogEntryId(1, 2), 38),
                new LogEntry<int>(new LogEntryId(1, 3), 98)
            };
            var request = new AppendRequest<int>
            {
                Term = 1,
                PreviousLogEntry = this.journal.LastLogEntryId,
                Entries = expectedEntries
            };
            var response = await this.role.Append(request);

            // Check that the call completed successfully and that the correct entry was written to the underlying log.
            response.Success.Should().BeTrue();
            response.Term.Should().Be(1);
            await this.journal.Received().AppendOrOverwrite(Arg.Any<IEnumerable<LogEntry<int>>>());
            this.journal.Entries.Skip(1).Should().BeEquivalentTo(expectedEntries);
        }
    }
}