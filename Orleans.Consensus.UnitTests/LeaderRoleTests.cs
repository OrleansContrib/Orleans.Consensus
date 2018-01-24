using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using FluentAssertions;
using FluentAssertions.Common;

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


namespace Orleans.Consensus.UnitTests
{
    public class LeaderRoleTests
    {
        private readonly IRoleCoordinator<int> coordinator;

        private readonly InMemoryLog<int> journal;

        private readonly InMemoryPersistentState persistentState;
        
        private readonly ILeaderRole<int> role;

        private readonly MockTimers timers;

        private readonly VolatileState volatileState;

        private readonly IServerIdentity identity;

        private readonly StaticMembershipProvider members;

        private readonly FakeGrainFactory grainFactory;

        public LeaderRoleTests(ITestOutputHelper output)
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddTransient(_ => Substitute.For<IRaftGrain<int>>());
            serviceCollection.AddLogging(loggingBuilder => loggingBuilder.AddProvider(new XunitLoggerProvider(output)));

            this.volatileState = new VolatileState();
            serviceCollection.AddSingleton<IRaftVolatileState>(this.volatileState);
            
            this.coordinator = Substitute.For<IRoleCoordinator<int>>();
            this.coordinator.StepDownIfGreaterTerm(Arg.Any<IMessage>())
                .Returns(
                    info => Task.FromResult(((IMessage)info[0]).Term > this.persistentState.CurrentTerm));
            var currentRole = Substitute.For<IRaftRole<int>>();
            currentRole.RequestVote(Arg.Any<RequestVoteRequest>())
                .Returns(Task.FromResult(new RequestVoteResponse { Term = 1, VoteGranted = true }));
            currentRole.Append(Arg.Any<AppendRequest<int>>())
                .Returns(Task.FromResult(new AppendResponse { Term = 1, Success = true }));
            this.coordinator.Role.Returns(currentRole);
            serviceCollection.AddSingleton(this.coordinator);

            this.timers = new MockTimers();
            serviceCollection.AddSingleton<RegisterTimerDelegate>(this.timers.RegisterTimer);

            this.persistentState = Substitute.ForPartsOf<InMemoryPersistentState>();
            serviceCollection.AddSingleton<IRaftPersistentState>(this.persistentState);

            this.journal = Substitute.ForPartsOf<InMemoryLog<int>>();
            serviceCollection.AddSingleton<IPersistentLog<int>>(this.journal);

            this.identity = Substitute.For<IServerIdentity>();
            this.identity.Id.Returns(Guid.NewGuid().ToString());
            serviceCollection.AddSingleton(this.identity);

            this.members = new StaticMembershipProvider(this.identity);
            this.members.SetServers(new[] { this.identity.Id, "other1", "other2", "other3", "other4" });
            serviceCollection.AddSingleton<IMembershipProvider>(this.members);

            serviceCollection.AddSingleton(Substitute.For<IStateMachine<int>>());
            serviceCollection.AddSingleton<IGrainFactory, FakeGrainFactory>();
            serviceCollection.AddTransient<ILeaderRole<int>, LeaderRole<int>>();
            
            // After the container is configured, resolve required services.
            var container = serviceCollection.BuildServiceProvider();
            this.role = container.GetRequiredService<ILeaderRole<int>>();
            this.grainFactory = container.GetRequiredService<IGrainFactory>() as FakeGrainFactory;

            this.OnRaftGrainCreated =
                (id, grain) =>
                grain.RequestVote(Arg.Any<RequestVoteRequest>())
                    .Returns(Task.FromResult(new RequestVoteResponse { VoteGranted = true }));
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
        /// Leaders should start an election timer when initializing.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task EntrySendsHeartbeat()
        {
            // Setup: configure grains to always grant votes
            this.OnRaftGrainCreated = (id, grain) =>
            {
                grain.Append(Arg.Any<AppendRequest<int>>())
                    .Returns(Task.FromResult(new AppendResponse { Term = 1, Success = true }));
            };

            this.persistentState.CurrentTerm.Returns(4);
            this.journal.Entries.Add(new LogEntry<int>(new LogEntryId(3, 1), 4));

            await this.role.Enter();

            // A timer should have been registered for the heartbeat;
            this.timers.Registrations.Should().HaveCount(1);
            
            // Fire the timer.
            await this.timers[0].Callback(this.timers[0].State);

            var expectedRequest = new AppendRequest<int>
            {
                Leader = this.identity.Id,
                Term = this.persistentState.CurrentTerm,
                LeaderCommitIndex = this.volatileState.CommitIndex,
                PreviousLogEntry = new LogEntryId(3, 1)
            };

            // Check that all of the followers received a heartbeat request.
            foreach (var server in this.members.OtherServers)
            {
                var grain = this.grainFactory.GetGrain<IRaftGrain<int>>(server);
                grain.ReceivedCalls().Should().ContainSingle().Which.IsSameOrEqualTo(expectedRequest);
            }
        }

        /// <summary>
        /// Leaders must dispose the election timer when transitioning out of the leader role.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task ExitDisposesTimer()
        {
            await this.EntrySendsHeartbeat();
            await this.role.Exit();

            foreach (var timer in this.timers)
            {
                timer.Disposable.Disposed.Should().BeTrue();
            }
        }

        /// <summary>
        /// Leaders transition into the follower role if they discover a higher term number in an append
        /// response.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task BecomeFollowerIfHigherTermDiscoveredInResponse()
        {
            this.OnRaftGrainCreated = (id, grain) =>
            {
                // All other servers are in a higher term.
                grain.Append(Arg.Any<AppendRequest<int>>())
                    .Returns(Task.FromResult(new AppendResponse { Term = 2, Success = false }));
            };

            await this.role.Enter();
            await this.role.ReplicateOperations(new[] { 293 });

            // Check that the leader would have stepped down.
            await
                this.coordinator.Received().StepDownIfGreaterTerm(Arg.Any<IMessage>());
        }

        /// <summary>
        /// Leaders learn of new terms when receiving append calls.
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
            await this.coordinator.Received().StepDownIfGreaterTerm(request);
        }
        
        /// <summary>
        /// Leaders do not append entries from deposed leaders.
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
                Entries = new [] { new LogEntry<int>(new LogEntryId(1, 1), 8) }
            };

            // Check that append fails.
            var response = await this.role.Append(request);
            response.Success.Should().BeFalse();
            response.Term.Should().Be(2);

            // Check that the no entries were written to the log.
            await this.journal.DidNotReceive().AppendOrOverwrite(Arg.Any<LogEntry<int>[]>());

            // Check that the role did not transition into a follower.
            await this.coordinator.DidNotReceive().BecomeFollowerForTerm(Arg.Any<long>());
        }

        /// <summary>
        /// Leaders can replicate operations.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task CanReplicateOperations()
        {
            this.OnRaftGrainCreated = (id, grain) =>
            { grain.Append(Arg.Any<AppendRequest<int>>()).Returns(new AppendResponse { Success = true }); };
            await this.role.Enter();
            var replicatedOperations = await this.role.ReplicateOperations(new[] { 89 });
            replicatedOperations.Should()
                .ContainSingle()
                .Which.IsSameOrEqualTo(new LogEntry<int>(new LogEntryId(0, 1), 89));
        }
        
        /// <summary>
        /// The role name is "Leader".
        /// </summary>
        [Fact]
        public void RoleNameIsLeader()
        {
            this.role.RoleName.Should().Be("Leader");
        }

        /// <summary>
        /// Leaders reject votes from leaders in previous terms.
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
        /// Leaders should learn of new terms in request vote calls.
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