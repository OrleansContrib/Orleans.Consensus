using Autofac;

namespace Orleans.Consensus.UnitTests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    
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

        private readonly InMemoryPersistentState persistentState;

        private readonly IRandom random;

        private readonly FollowerRole<int> role;

        private readonly ISettings settings;

        private readonly IStateMachine<int> stateMachine;

        private readonly MockTimers timers;

        private readonly VolatileState volatileState;

        public FollowerRoleTests(ITestOutputHelper output)
        {
            var builder = new ContainerBuilder();
            builder.RegisterInstance<ILogger>(new TestLogger(output));

            this.volatileState = new VolatileState();
            builder.RegisterInstance<IRaftVolatileState>(this.volatileState);

            // Configure settings
            this.settings = Substitute.For<ISettings>();
            this.settings.ApplyEntriesOnFollowers.Returns(true);
            this.settings.MinElectionTimeoutMilliseconds.Returns(MinElectionTime);
            this.settings.MaxElectionTimeoutMilliseconds.Returns(MaxElectionTime);
            builder.RegisterInstance(this.settings);

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

            this.persistentState = Substitute.ForPartsOf<InMemoryPersistentState>();
            builder.RegisterInstance<IRaftPersistentState>(this.persistentState);

            this.journal = Substitute.ForPartsOf<InMemoryLog<int>>();
            builder.RegisterInstance<IPersistentLog<int>>(this.journal);

            builder.RegisterType<FollowerRole<int>>().AsImplementedInterfaces().AsSelf();

            // After the container is configured, resolve required services.
            this.role = builder.Build().Resolve<FollowerRole<int>>();
        }

        /// <summary>
        /// Followers should start an election timer when initializing.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
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

        /// <summary>
        /// Followers must dispose the election timer when transitioning out of the follower role.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task ExitDisposesTimer()
        {
            await this.EntryStartsElectionTimer();
            await this.role.Exit();

            foreach (var timer in this.timers)
            {
                timer.Disposable.Disposed.Should().BeTrue();
            }
        }

        /// <summary>
        /// Followers transition into candidates when not messaged between election timer firings.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
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

        /// <summary>
        /// Entries are written to the followers log during a valid append calls.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task ValidEntriesAreWrittenToLog()
        {
            this.persistentState.CurrentTerm.Returns(_ => 1);
            this.journal.Entries.Add(new LogEntry<int>(new LogEntryId(1, 1), 27));

            // Become a follower
            await this.role.Enter();

            // Append some entries.
            var request = new AppendRequest<int>
            {
                Term = 1,
                PreviousLogEntry = this.journal.LastLogEntryId,
                Entries =
                    new []
                    {
                        new LogEntry<int>(new LogEntryId(1, 2), 38),
                        new LogEntry<int>(new LogEntryId(1, 3), 98)
                    },
                Leader = "Homer",
                LeaderCommitIndex = 0
            };
            var response = await this.role.Append(request);

            // Check that the call succeeded and that the entries were written to the log.
            response.Success.Should().BeTrue();
            response.Term.Should().Be(1);
            await this.journal.Received().AppendOrOverwrite(Arg.Any<LogEntry<int>[]>());
            this.journal.Entries.Skip(1).Should().BeEquivalentTo(request.Entries);

            // If a follower is in the same term as the leader, a call to UpdateTermAndVote should not occur.
            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());
        }

        /// <summary>
        /// Append can only succeed if the previous entry is present in a follower's log.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task DenyAppendIfPreviousEntryNotInLog()
        {
            // Start a follower with some log entries.
            this.persistentState.CurrentTerm.Returns(_ => 2);
            this.journal.Entries.AddRange(
                new[] { new LogEntry<int>(new LogEntryId(1, 1), 27), new LogEntry<int>(new LogEntryId(2, 2), 45) });
            await this.role.Enter();

            // Attempt to append an entry when the previous entry is not in the log.
            var request = new AppendRequest<int>
            {
                Term = 2,
                PreviousLogEntry = new LogEntryId(2, 3),
                Entries = new [] { new LogEntry<int>(new LogEntryId(2, 4), 38) }
            };
            var response = await this.role.Append(request);

            // Check that the call failed and that no new entries were added to the log.
            response.Success.Should().BeFalse();
            response.Term.Should().Be(2);
            response.LastLogEntryId.Should().Be(this.journal.LastLogEntryId);
            await this.journal.DidNotReceive().AppendOrOverwrite(Arg.Any<LogEntry<int>[]>());
            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());
        }

        /// <summary>
        /// Followers learn of new terms when receiving append calls.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task LearnsOfNewTermThroughAppend()
        {
            // Start a follower in term 1.
            this.persistentState.CurrentTerm.Returns(_ => 1);
            this.journal.Entries.AddRange(new[] { new LogEntry<int>(new LogEntryId(1, 1), 27) });
            await this.role.Enter();

            // Append an entry from a new term.
            var request = new AppendRequest<int>
            {
                Term = 2,
                PreviousLogEntry = new LogEntryId(1, 1),
                Entries = new [] { new LogEntry<int>(new LogEntryId(2, 2), 38) }
            };
            var response = await this.role.Append(request);

            // Check that the call succeeded and that the log entries were written.
            response.Success.Should().BeTrue();
            response.Term.Should().Be(2);
            response.LastLogEntryId.Should().Be(this.journal.LastLogEntryId);
            await this.journal.Received().AppendOrOverwrite(Arg.Any<LogEntry<int>[]>());
            this.journal.Entries.Skip(1).Should().BeEquivalentTo(request.Entries);

            // Ensure that the term was updated.
            await this.persistentState.Received().UpdateTermAndVote(null, 2);
        }

        /// <summary>
        /// Deposed leaders cannot append entries.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task DenyAppendFromPreviousTerm()
        {
            // Start a follower with some log entries.
            this.persistentState.CurrentTerm.Returns(_ => 2);
            this.journal.Entries.AddRange(new[] { new LogEntry<int>(new LogEntryId(1, 1), 27) });
            await this.role.Enter();

            // Attempt to append an entry from a deposed leader.
            var request = new AppendRequest<int>
            {
                Term = 1,
                PreviousLogEntry = new LogEntryId(1, 1),
                Entries = new [] { new LogEntry<int>(new LogEntryId(1, 2), 38) }
            };
            var response = await this.role.Append(request);

            // Check that the call failed and that no new entries were added to the log.
            response.Success.Should().BeFalse();
            response.Term.Should().Be(2);
            response.LastLogEntryId.Should().Be(this.journal.LastLogEntryId);
            await this.journal.DidNotReceive().AppendOrOverwrite(Arg.Any<LogEntry<int>[]>());
        }

        /// <summary>
        /// Append calls must overwrite conflicting entries.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task AppendRemovesConflictingEntries()
        {
            // Start a follower with some log entries.
            this.persistentState.CurrentTerm.Returns(_ => 2);
            this.journal.Entries.AddRange(new[] { new LogEntry<int>(new LogEntryId(1, 1), 27) });
            await this.role.Enter();

            // Attempt to append an entry when the previous entry is not in the log.
            var request = new AppendRequest<int>
            {
                Term = 2,
                PreviousLogEntry = default(LogEntryId),
                Entries = new [] { new LogEntry<int>(new LogEntryId(2, 1), 38) }
            };
            var response = await this.role.Append(request);

            // Check that the call succeeded and that the old entry was replaced.
            response.Success.Should().BeTrue();
            response.Term.Should().Be(2);
            response.LastLogEntryId.Should().Be(this.journal.LastLogEntryId);
            await this.journal.Received().AppendOrOverwrite(request.Entries);
            this.journal.Entries.Count.Should().Be(1);
            this.journal.Entries[0].Should().Be(request.Entries[0]);
        }

        /// <summary>
        /// Followers cannot replicate operations.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task CannotReplicateOperations()
        {
            await this.role.Enter();
            var exception = await Assert.ThrowsAsync<NotLeaderException>(async () => await this.role.ReplicateOperations(new[] { 1 }));
            Assert.True(string.IsNullOrEmpty(exception.Leader));
        }

        /// <summary>
        /// The follower learns of the leader's id through valid append calls.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task LearnsOfLeaderIdThroughAppend()
        {
            await this.ValidEntriesAreWrittenToLog();
            var exception = await Assert.ThrowsAsync<NotLeaderException>(async () => await this.role.ReplicateOperations(new[] { 1 }));
            Assert.Equal("Homer", this.volatileState.LeaderId);
            Assert.Equal("Homer", exception.Leader);
        }

        /// <summary>
        /// The role name is "Follower".
        /// </summary>
        [Fact]
        public void RoleNameIsFollower()
        {
            this.role.RoleName.Should().Be("Follower");
        }

        /// <summary>
        /// Committed entries must be applied to state machines.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task CommittedEntriesAreAppliedToStateMachine()
        {
            await this.AppendValidEntriesWithSomeCommitted();

            // Check that the operations were applied to the state machine.
            Received.InOrder(
                () =>
                {
                    this.stateMachine.Received().Apply(Arg.Is(this.journal.Entries[0]));
                    this.stateMachine.Received().Apply(Arg.Is(this.journal.Entries[1]));
                });
            this.settings.ApplyEntriesOnFollowers.Returns(true);
        }

        /// <summary>
        /// Entries must not be applies to the state machine if settings prohibit application on followers.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task EntriesAreNotAppliedToStateMachineIfProhibitedBySettings()
        {
            this.settings.ApplyEntriesOnFollowers.Returns(false);
            await this.AppendValidEntriesWithSomeCommitted();

            // Check that the operations were not applied to the state machine.
            await this.stateMachine.DidNotReceive().Apply(Arg.Is(this.journal.Entries[0]));
            await this.stateMachine.DidNotReceive().Apply(Arg.Is(this.journal.Entries[1]));
        }

        /// <summary>
        /// Followers reject votes from candidates in previous terms.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task RejectVotesFromPreviousTerms()
        {
            this.persistentState.CurrentTerm.Returns(_ => 2);
            await this.role.Enter();

            var response = await this.role.RequestVote(new RequestVoteRequest(1, "Trump", default(LogEntryId)));
            response.Term.Should().Be(2);
            response.VoteGranted.Should().BeFalse();
            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());
        }

        /// <summary>
        /// Followers may only vote for one candidate per term.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task RejectVotesWhenAlreadyVoted()
        {
            this.persistentState.CurrentTerm.Returns(_ => 2);
            this.persistentState.VotedFor.Returns(_ => "Napoleon");
            await this.role.Enter();

            var response = await this.role.RequestVote(new RequestVoteRequest(2, "Kennedy", default(LogEntryId)));
            response.Term.Should().Be(2);
            response.VoteGranted.Should().BeFalse();
            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());
        }

        /// <summary>
        /// Followers reject candidates when the local log is more up-to-date than the candidate's log.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task RejectVotesWhenLocalLogIsMoreUpToDate()
        {
            this.journal.Entries.Add(new LogEntry<int>(new LogEntryId(1, 1), 888));
            this.persistentState.CurrentTerm.Returns(_ => 2);
            this.persistentState.VotedFor.Returns(_ => null);
            await this.role.Enter();

            var response = await this.role.RequestVote(new RequestVoteRequest(2, "Kennedy", default(LogEntryId)));
            response.Term.Should().Be(2);
            response.VoteGranted.Should().BeFalse();
            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());
        }

        /// <summary>
        /// Followers vote for candidates they have already voted for this term.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task AcceptVoteWhenAlreadyVotedForThatCandidate()
        {
            this.persistentState.CurrentTerm.Returns(_ => 2);
            this.persistentState.VotedFor.Returns(_ => "Napoleon");
            await this.role.Enter();

            var response = await this.role.RequestVote(new RequestVoteRequest(2, "Napoleon", default(LogEntryId)));
            response.Term.Should().Be(2);
            response.VoteGranted.Should().BeTrue();
            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());
        }

        /// <summary>
        /// Followers vote for candidates if they have not yet voted this term.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task AcceptVotesWhenHaveNotVoted()
        {
            this.persistentState.CurrentTerm.Returns(_ => 2);
            this.persistentState.VotedFor.Returns(_ => null);
            await this.role.Enter();

            var response = await this.role.RequestVote(new RequestVoteRequest(2, "Napoleon", default(LogEntryId)));
            response.Term.Should().Be(2);
            response.VoteGranted.Should().BeTrue();
            await this.persistentState.Received().UpdateTermAndVote("Napoleon", 2);
        }

        /// <summary>
        /// Followers should learn of new terms in request vote calls.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task LearnOfNewTermThroughRequestVote()
        {
            this.persistentState.CurrentTerm.Returns(_ => 1);
            this.persistentState.VotedFor.Returns(_ => null);
            await this.role.Enter();

            var response = await this.role.RequestVote(new RequestVoteRequest(2, "Napoleon", default(LogEntryId)));
            response.Term.Should().Be(2);
            response.VoteGranted.Should().BeTrue();

            // Ensure that the term was updated.
            await this.persistentState.Received().UpdateTermAndVote("Napoleon", 2);
        }

        /// <summary>
        /// Appends some entries to the log, specifying that some entries are also committed on the leader.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        private async Task AppendValidEntriesWithSomeCommitted()
        {
            this.persistentState.CurrentTerm.Returns(_ => 1);
            this.journal.Entries.Add(new LogEntry<int>(new LogEntryId(1, 1), 27));

            // Become a follower
            await this.role.Enter();

            // Append some entries.
            var request = new AppendRequest<int>
            {
                Term = 1,
                PreviousLogEntry = this.journal.LastLogEntryId,
                Entries =
                    new []
                    {
                        new LogEntry<int>(new LogEntryId(1, 2), 38),
                        new LogEntry<int>(new LogEntryId(1, 3), 98)
                    },
                LeaderCommitIndex = 2
            };
            var response = await this.role.Append(request);

            // Check that the call succeeded and that the entries were written to the log.
            response.Success.Should().BeTrue();
            response.Term.Should().Be(1);
            await this.journal.Received().AppendOrOverwrite(Arg.Any<LogEntry<int>[]>());
            this.journal.Entries.Skip(1).Should().BeEquivalentTo(request.Entries);

            // If a follower is in the same term as the leader, a call to UpdateTermAndVote should not occur.
            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());
        }
    }
}