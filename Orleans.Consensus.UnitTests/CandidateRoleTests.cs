namespace Orleans.Consensus.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using Autofac;

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

    public class CandidateRoleTests
    {
        private const int MinElectionTime = 72;

        private const int MaxElectionTime = 720;

        private const int RiggedRandomResult = 440;

        private readonly IRoleCoordinator<int> coordinator;

        private readonly InMemoryLog<int> journal;

        private readonly InMemoryPersistentState persistentState;

        private readonly IRandom random;

        private readonly CandidateRole<int> role;

        private readonly ISettings settings;

        private readonly MockTimers timers;

        private readonly VolatileState volatileState;

        private readonly IServerIdentity identity;

        private readonly StaticMembershipProvider members;

        private readonly FakeGrainFactory grainFactory;

        private readonly AutoSubstitute container;

        public CandidateRoleTests(ITestOutputHelper output)
        {
            var builder =
                new AutoSubstitute(cb => cb.Register(_ => Substitute.For<IRaftGrain<int>>()).InstancePerDependency());
            builder.Provide<ILogger>(new TestLogger(output));

            this.volatileState = new VolatileState();
            builder.Provide<IRaftVolatileState>(this.volatileState);

            // Configure settings
            this.settings = builder.Resolve<ISettings>();
            this.settings.MinElectionTimeoutMilliseconds.Returns(MinElectionTime);
            this.settings.MaxElectionTimeoutMilliseconds.Returns(MaxElectionTime);

            // Rig random number generator to always return the same value.
            this.random = builder.Resolve<IRandom>();
            this.random.Next(Arg.Any<int>(), Arg.Any<int>()).Returns(RiggedRandomResult);

            this.coordinator = builder.Resolve<IRoleCoordinator<int>>();
            this.coordinator.StepDownIfGreaterTerm(Arg.Any<IMessage>())
                .Returns(
                    info => Task.FromResult(((IMessage)info[0]).Term > this.persistentState.CurrentTerm));
            var currentRole = builder.Resolve<IRaftRole<int>>();
            currentRole.RequestVote(Arg.Any<RequestVoteRequest>())
                .Returns(Task.FromResult(new RequestVoteResponse { Term = 1, VoteGranted = true }));
            currentRole.Append(Arg.Any<AppendRequest<int>>())
                .Returns(Task.FromResult(new AppendResponse { Term = 1, Success = true }));
            this.coordinator.Role.Returns(currentRole);

            this.timers = new MockTimers();
            builder.Provide<RegisterTimerDelegate>(this.timers.RegisterTimer);

            this.persistentState = Substitute.ForPartsOf<InMemoryPersistentState>();
            builder.Provide<IRaftPersistentState>(this.persistentState);

            this.journal = Substitute.ForPartsOf<InMemoryLog<int>>();
            builder.Provide<IPersistentLog<int>>(this.journal);

            this.identity = Substitute.For<IServerIdentity>();
            this.identity.Id.Returns(Guid.NewGuid().ToString());
            builder.Provide(this.identity);

            this.members = builder.Resolve<StaticMembershipProvider>();
            this.members.SetServers(new[] { this.identity.Id, "other1", "other2", "other3", "other4" });
            builder.Provide<IMembershipProvider>(this.members);

            this.grainFactory = new FakeGrainFactory(builder.Container);
            builder.Provide<IGrainFactory>(this.grainFactory);
            this.OnRaftGrainCreated =
                (id, grain) =>
                grain.RequestVote(Arg.Any<RequestVoteRequest>())
                    .Returns(Task.FromResult(new RequestVoteResponse { VoteGranted = true }));

            // After the container is configured, resolve required services.
            this.role = builder.Resolve<CandidateRole<int>>();
            this.container = builder;
        }

        private Action<string, IRaftGrain<int>> OnRaftGrainCreated
        {
            set
            {
                this.grainFactory.OnGrainCreated = (id, grain) =>
                {
                    var raftGrain = grain as IRaftGrain<int>;

                    var primaryKey = id as string;
                    if (raftGrain == null || string.IsNullOrWhiteSpace(primaryKey))
                    {
                        return;
                    }

                    value(primaryKey, raftGrain);
                };
            }
        }

        /// <summary>
        /// Candidates should start an election timer when initializing.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task EntryStartsElectionTimer()
        {
            await this.role.Enter();

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
        /// Candidates should request votes from other servers when initializing.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task RequestsVotesOnEntry()
        {
            // Setup: configure grains to always grant votes
            this.OnRaftGrainCreated = (id, grain) =>
            {
                var raftGrain = grain;
                raftGrain?.RequestVote(Arg.Any<RequestVoteRequest>())
                    .Returns(Task.FromResult(new RequestVoteResponse { Term = 1, VoteGranted = true }));
            };

            await this.role.Enter();

            foreach (var server in this.members.AllServers)
            {
                var grain = this.grainFactory.GetGrain<IRaftGrain<int>>(server);
                if (string.Equals(server, this.identity.Id, StringComparison.Ordinal))
                {
                    await grain.DidNotReceive().RequestVote(Arg.Any<RequestVoteRequest>());
                    continue;
                }

                var request = new RequestVoteRequest(
                    this.persistentState.CurrentTerm,
                    this.identity.Id,
                    this.journal.LastLogEntryId);

                await grain.Received().RequestVote(request);
            }
        }

        /// <summary>
        /// Candidates must dispose the election timer when transitioning out of the candidate role.
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
        /// Candidates transition into the leader role when they receive a quorum of votes.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task BecomeLeaderUponQuorum()
        {
            this.OnRaftGrainCreated = (id, grain) =>
            {
                // Two of the 4 other servers return true, making a quorum given that candidates
                // always vote for themselves.
                var grantVote = id == "other1" || id == "other2";
                grain.RequestVote(Arg.Any<RequestVoteRequest>())
                    .Returns(Task.FromResult(new RequestVoteResponse { Term = 1, VoteGranted = grantVote }));
            };

            await this.role.Enter();

            // Check that the candidate transitioned into a leader.
            await this.coordinator.Received().BecomeLeader();
        }

        /// <summary>
        /// Candidates transition into the follower role if they discover a higher term number in a RequestVote
        /// response.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task BecomeFollowerIfHigherTermDiscoveredInResponse()
        {
            this.OnRaftGrainCreated = (id, grain) =>
            {
                // All other servers are in a higher term.
                grain.RequestVote(Arg.Any<RequestVoteRequest>())
                    .Returns(Task.FromResult(new RequestVoteResponse { Term = 2, VoteGranted = false }));
            };

            await this.role.Enter();

            // Check that the candidate would have stepped down.
            await
                this.coordinator.Received().StepDownIfGreaterTerm(Arg.Any<IMessage>());
        }

        /// <summary>
        /// Candidates transition into candidates for the next term when not messaged between election timer firings.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task RestartCandidacyWhenNoLeaderDeclared()
        {
            this.OnRaftGrainCreated = (id, grain) =>
            { grain.RequestVote(Arg.Any<RequestVoteRequest>()).Returns(new RequestVoteResponse { Term = 1 }); };

            // Become a candidate.
            await this.role.Enter();

            // Fire the election timer.
            await this.timers[0].Callback(null);
            await this.coordinator.Received().BecomeCandidate();
        }

        /// <summary>
        /// Candidates learn of new terms when receiving append calls.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task LearnsOfNewTermThroughAppend()
        {
            await this.role.Enter();
            this.persistentState.CurrentTerm.Returns(_ => 1);
            this.persistentState.VotedFor.Returns(_ => this.identity.Id);

            this.coordinator.ClearReceivedCalls();
            var request = new AppendRequest<int> { Term = 2 };
            await this.role.Append(request);
            await this.coordinator.Received().BecomeFollowerForTerm(2);
        }

        /// <summary>
        /// Candidates learn of new leaders when receiving append calls.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task LearnsOfNewLeaderThroughAppend()
        {
            await this.role.Enter();
            this.persistentState.CurrentTerm.Returns(_ => 1);
            this.persistentState.VotedFor.Returns(_ => this.identity.Id);

            this.coordinator.ClearReceivedCalls();
            var request = new AppendRequest<int> { Term = 1 };
            await this.role.Append(request);
            await this.coordinator.Received().BecomeFollowerForTerm(1);
        }
        
        /// <summary>
        /// Candidates do not append entries from deposed leaders.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task DoesNotAppendEntries()
        {
            await this.role.Enter();
            this.persistentState.CurrentTerm.Returns(_ => 2);
            this.persistentState.VotedFor.Returns(_ => this.identity.Id);

            this.coordinator.ClearReceivedCalls();
            var request = new AppendRequest<int>
            {
                Term = 1,
                Entries = new List<LogEntry<int>> { new LogEntry<int>(new LogEntryId(1, 1), 8) }
            };

            // Check that append fails.
            var response = await this.role.Append(request);
            response.Success.Should().BeFalse();
            response.Term.Should().Be(2);

            // Check that the no entries were written to the log.
            await this.journal.DidNotReceive().AppendOrOverwrite(Arg.Any<IEnumerable<LogEntry<int>>>());

            // Check that the role did not transition into a follower.
            await this.coordinator.DidNotReceive().BecomeFollowerForTerm(Arg.Any<long>());
        }

        /// <summary>
        /// Candidates cannot replicate operations.
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
        /// The role name is "Candidate".
        /// </summary>
        [Fact]
        public void RoleNameIsCandidate()
        {
            this.role.RoleName.Should().Be("Candidate");
        }

        /// <summary>
        /// Candidates reject votes from candidates in previous terms.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task RejectsVotes()
        {
            await this.role.Enter();
            this.persistentState.CurrentTerm.Returns(_ => 2);
            this.persistentState.VotedFor.Returns(_ => this.identity.Id);

            this.persistentState.ClearReceivedCalls();
            var response = await this.role.RequestVote(new RequestVoteRequest(2, "Trump", default(LogEntryId)));
            response.Term.Should().Be(2);
            response.VoteGranted.Should().BeFalse();
            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());
        }

        /// <summary>
        /// Candidates should learn of new terms in request vote calls.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task LearnOfNewTermThroughRequestVote()
        {
            await this.role.Enter();
            this.persistentState.CurrentTerm.Returns(_ => 1);
            this.persistentState.VotedFor.Returns(_ => this.identity.Id);

            var request = new RequestVoteRequest(2, "Napoleon", default(LogEntryId));
            await this.role.RequestVote(request);
            await this.coordinator.Received().StepDownIfGreaterTerm(request);
        }
    }
}