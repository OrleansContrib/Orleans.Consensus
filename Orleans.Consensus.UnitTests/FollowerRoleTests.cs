using System;
using System.Threading.Tasks;

namespace Orleans.Consensus.UnitTests
{
    using System.Collections.Generic;
    using System.Linq;

    using Autofac;

    using FluentAssertions;

    using NSubstitute;

    using Orleans.Consensus.Actors;
    using Orleans.Consensus.Contract;
    using Orleans.Consensus.Contract.Log;
    using Orleans.Consensus.Contract.Messages;
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

        private readonly ISettings settings;

        private readonly IRandom random;

        private readonly IRoleCoordinator<int> coordinator;

        private readonly IStateMachine<int> stateMachine;

        private readonly MockTimers timers;

        private readonly FollowerRole<int> role;

        private readonly IRaftPersistentState persistentState;

        private readonly IPersistentLog<int> journal;

        private readonly IContainer container;

        public FollowerRoleTests(ITestOutputHelper output)
        {
            var builder = new ContainerBuilder();
            builder.RegisterInstance<ILogger>(new TestLogger(output));

            // Configure settings
            this.settings = Substitute.For<ISettings>();
            this.settings.ApplyEntriesOnFollowers.Returns(true);
            this.settings.MinElectionTimeoutMilliseconds.Returns(MinElectionTime);
            this.settings.MaxElectionTimeoutMilliseconds.Returns(MaxElectionTime);

            // Rig random number generator to always return the same value.
            this.random = Substitute.For<IRandom>();
            this.random.Next(Arg.Any<int>(), Arg.Any<int>()).Returns(RiggedRandomResult);
            builder.RegisterInstance(this.random);

            this.coordinator = Substitute.For<IRoleCoordinator<int>>();
            builder.RegisterInstance(this.coordinator);

            this.stateMachine = Substitute.For<IStateMachine<int>>();
            builder.RegisterInstance(this.stateMachine);

            this.timers = new MockTimers();
            builder.RegisterInstance<RegisterTimerDelegate>(this.timers.RegisterTimer);

            this.persistentState = Substitute.For<IRaftPersistentState>();
            builder.RegisterInstance(this.persistentState);

            this.journal = Substitute.For<IPersistentLog<int>>();
            builder.RegisterInstance(this.journal);

            // After the container is configured, resolve required services.
            this.container = builder.Build();
            this.role = this.container.Resolve<FollowerRole<int>>();
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
            this.random.Received().Next(MinElectionTime, MaxElectionTime);
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
            this.journal.Contains(default(LogEntryId)).Returns(true);
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
            // Setup: capture log entries written
            List<LogEntry<int>> actualEntries = null;
            this.journal.AppendOrOverwrite(Arg.Any<IEnumerable<LogEntry<int>>>())
                .Returns(Task.FromResult(0))
                .AndDoes(_ => actualEntries = ((IEnumerable<LogEntry<int>>)_[0]).ToList());

            // Become a follower
            await this.role.Enter();

            // Receive a valid append message.
            this.persistentState.CurrentTerm.Returns(_ => 1);
            var previous = new LogEntryId(1, 2);
            this.journal.Contains(previous).Returns(true);

            // Append the entry 1.3 with a payload of 38
            var expectedEntries = new List<LogEntry<int>>
            {
                new LogEntry<int>(new LogEntryId(1, 3), 38),
                new LogEntry<int>(new LogEntryId(1, 5), 98)
            };
            var request = new AppendRequest<int> { Term = 1, PreviousLogEntry = previous, Entries = expectedEntries };
            var response = await this.role.Append(request);

            // Check that the call completed successfully and that the correct entry was written to the underlying log.
            response.Success.Should().BeTrue();
            response.Term.Should().Be(1);
            await this.journal.Received().AppendOrOverwrite(Arg.Any<IEnumerable<LogEntry<int>>>());
            actualEntries.Should().BeEquivalentTo(expectedEntries);
        }
    }
}
