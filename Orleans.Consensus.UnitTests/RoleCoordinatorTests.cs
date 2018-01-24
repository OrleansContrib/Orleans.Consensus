using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

using FluentAssertions;

using NSubstitute;
using Orleans.Consensus.Contract.Messages;
using Orleans.Consensus.Roles;
using Orleans.Consensus.State;
using Orleans.Consensus.UnitTests.Utilities;

using Xunit;
using Xunit.Abstractions;

namespace Orleans.Consensus.UnitTests
{
    public class RoleCoordinatorTests
    {
        private readonly IRoleCoordinator<int> coordinator;

        private readonly InMemoryPersistentState persistentState;

        public RoleCoordinatorTests(ITestOutputHelper output)
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging(loggingBuilder => loggingBuilder.AddProvider(new XunitLoggerProvider(output)));

            this.persistentState = Substitute.ForPartsOf<InMemoryPersistentState>();
            serviceCollection.AddSingleton<IRaftPersistentState>(this.persistentState);

            serviceCollection.AddSingleton(Substitute.For<IFollowerRole<int>>());
            serviceCollection.AddSingleton(Substitute.For<ILeaderRole<int>>());
            serviceCollection.AddSingleton(Substitute.For<ICandidateRole<int>>());

            // After the container is configured, resolve required services.
            serviceCollection.AddSingleton<RoleCoordinator<int>>();
            var serviceProvider = serviceCollection.BuildServiceProvider();
            this.coordinator = serviceProvider.GetRequiredService<RoleCoordinator<int>>();
        }

        /// <summary>
        /// When servers start up, they begin as followers. (§5.2)
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task InitializesAsFollower()
        {
            await this.coordinator.Initialize();
            this.coordinator.Role.Should().NotBeNull();
            this.coordinator.Role.Should().BeAssignableTo<IFollowerRole<int>>();
            await this.coordinator.Role.Received().Enter();
        }

        /// <summary>
        /// When <see cref="IRoleCoordinator{TOperation}.BecomeCandidate"/> is called, the coordinator should transition
        /// to the <see cref="ICandidateRole{TOperation}"/>.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task TransitionToCandidate()
        {
            await this.coordinator.Initialize();
            var initialRole = this.coordinator.Role;
            await this.coordinator.BecomeCandidate();

            await initialRole.Received().Exit();
            this.coordinator.Role.Should().BeAssignableTo<ICandidateRole<int>>();
            await this.coordinator.Role.Received().Enter();
        }

        /// <summary>
        /// When <see cref="IRoleCoordinator{TOperation}.BecomeLeader"/> is called, the coordinator should transition
        /// to the <see cref="ILeaderRole{TOperation}"/>.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task TransitionToLeader()
        {
            await this.coordinator.Initialize();
            var initialRole = this.coordinator.Role;
            await this.coordinator.BecomeLeader();

            await initialRole.Received().Exit();
            this.coordinator.Role.Should().BeAssignableTo<ILeaderRole<int>>();
            await this.coordinator.Role.Received().Enter();
        }

        /// <summary>
        /// When shutdown is called, the coordinator should transition to a null role and exit the previous role.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task ShutdownTransitionsToNull()
        {
            await this.coordinator.Initialize();
            var initialRole = this.coordinator.Role;
            await this.coordinator.Shutdown();

            await initialRole.Received().Exit();
            this.coordinator.Role.Should().BeNull();
        }

        /// <summary>
        /// Becoming a follower for a given term should write state prior to transitioning.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task BecomeFollowerForTerm()
        {
            await this.coordinator.Initialize();
            var initialRole = this.coordinator.Role;
            this.persistentState.CurrentTerm.Returns(2);
            initialRole.ClearReceivedCalls();
            await this.coordinator.BecomeFollowerForTerm(9);

            await this.persistentState.Received().UpdateTermAndVote(null, 9);

            await initialRole.Received().Exit();
            this.coordinator.Role.Should().BeAssignableTo<IFollowerRole<int>>();
            await this.coordinator.Role.Received().Enter();
        }

        /// <summary>
        /// Attempting to become a follower for a previous term is an invalid operation.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task BecomeFollowerForPreviousTermFails()
        {
            await this.coordinator.Initialize();
            var initialRole = this.coordinator.Role;
            this.persistentState.CurrentTerm.Returns(2);
            initialRole.ClearReceivedCalls();

            await
                Assert.ThrowsAsync<InvalidOperationException>(
                    async () => await this.coordinator.BecomeFollowerForTerm(1));

            await initialRole.DidNotReceive().Exit();
        }

        /// <summary>
        /// When becoming a follower for the current term, no state should be written.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task BecomeFollowerForCurrentTermDoesNotWriteState()
        {
            await this.coordinator.Initialize();
            var initialRole = this.coordinator.Role;
            this.persistentState.CurrentTerm.Returns(1);
            initialRole.ClearReceivedCalls();
            await this.coordinator.BecomeFollowerForTerm(1);

            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());

            await initialRole.Received().Exit();
            this.coordinator.Role.Should().BeAssignableTo<IFollowerRole<int>>();
            await this.coordinator.Role.Received().Enter();
        }

        /// <summary>
        /// <see cref="IRoleCoordinator{TOperation}.StepDownIfGreaterTerm{T}"/> transitions to follower if the given term
        /// is greater.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task StepsDownWhenTermIsGreater()
        {
            await this.coordinator.Initialize();
            await this.coordinator.BecomeLeader();
            var initialRole = this.coordinator.Role;
            this.persistentState.CurrentTerm.Returns(2);
            initialRole.ClearReceivedCalls();

            var message = Substitute.For<IMessage>();
            message.Term.Returns(99);
            await this.coordinator.StepDownIfGreaterTerm(message);

            await this.persistentState.Received().UpdateTermAndVote(null, 99);

            await initialRole.Received().Exit();
            this.coordinator.Role.Should().BeAssignableTo<IFollowerRole<int>>();
            await this.coordinator.Role.Received().Enter();
        }

        /// <summary>
        /// <see cref="IRoleCoordinator{TOperation}.StepDownIfGreaterTerm{T}"/> does not transition to follower if the
        /// given term is not greater.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        [Fact]
        public async Task DoesNotStepsDownWhenTermIsNotGreater()
        {
            await this.coordinator.Initialize();
            await this.coordinator.BecomeLeader();
            var initialRole = this.coordinator.Role;
            this.persistentState.CurrentTerm.Returns(2);
            initialRole.ClearReceivedCalls();

            // Test with same term.
            var message = Substitute.For<IMessage>();
            message.Term.Returns(2);
            await this.coordinator.StepDownIfGreaterTerm(message);

            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());

            await initialRole.DidNotReceive().Exit();
            this.coordinator.Role.Should().BeAssignableTo<ILeaderRole<int>>();

            // Test with previous term.
            message.Term.Returns(1);
            await this.coordinator.StepDownIfGreaterTerm(message);

            await this.persistentState.DidNotReceive().UpdateTermAndVote(Arg.Any<string>(), Arg.Any<long>());

            await initialRole.DidNotReceive().Exit();
            this.coordinator.Role.Should().BeAssignableTo<ILeaderRole<int>>();
        }
    }
}