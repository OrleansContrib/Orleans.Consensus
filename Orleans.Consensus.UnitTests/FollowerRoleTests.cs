using System;
using System.Threading.Tasks;

namespace Orleans.Consensus.UnitTests
{
    using AutofacContrib.NSubstitute;

    using FluentAssertions;

    using NSubstitute;

    using Orleans.Consensus.Actors;
    using Orleans.Consensus.Roles;

    using Xunit;

    public class FollowerRoleTests
    {
        private readonly ISettings settings;

        private readonly IRandom random;

        private readonly IRoleCoordinator<int> coordinator;

        private readonly IStateMachine<int> stateMachine;

        private readonly MockTimers timers;

        private readonly FollowerRole<int> role;

        public FollowerRoleTests()
        {
            var autoSubstitute = new AutoSubstitute();

            // Configure settings
            this.settings = autoSubstitute.Resolve<ISettings>();
            this.settings.ApplyEntriesOnFollowers.Returns(true);
            this.settings.MinElectionTimeoutMilliseconds.Returns(72);
            this.settings.MaxElectionTimeoutMilliseconds.Returns(720);

            // Rig random number generator to always return the same value.
            this.random = autoSubstitute.Resolve<IRandom>();
            this.random.Next(Arg.Any<int>(), Arg.Any<int>()).Returns(440);

            this.coordinator = Substitute.For<IRoleCoordinator<int>>();
            autoSubstitute.Provide(this.coordinator);

            this.stateMachine = Substitute.For<IStateMachine<int>>();
            autoSubstitute.Provide(this.stateMachine);

            this.timers = new MockTimers();
            autoSubstitute.Provide<RegisterTimerDelegate>(this.timers.RegisterTimer);

            this.role = autoSubstitute.Resolve<FollowerRole<int>>();
        }

        [Fact]
        public async Task TestEntry()
        {
            await this.role.Enter();
            
            // The state machine should have been reset.
            await this.stateMachine.Received().Reset();

            // A timer should have been registered.
            this.timers.Registrations.Should().HaveCount(1);

            // Check that the correct timer was registered.
            var timer = this.timers.Registrations[0];
            this.random.Received().Next(72, 720);
            timer.DueTime.Should().Be(TimeSpan.FromMilliseconds(440));
            timer.Period.Should().Be(TimeSpan.FromMilliseconds(440));
            timer.Disposable.Disposed.Should().BeFalse();

            // Check that the timer causes the instance to become a candidate.
            await this.coordinator.DidNotReceive().BecomeCandidate();
            await this.timers.Registrations[0].Callback(null);
            await this.coordinator.Received().BecomeCandidate();
        }

        [Fact]
        public async Task TestExit()
        {
            await this.TestEntry();
            await this.role.Exit();

            this.timers.Registrations[0].Disposable.Disposed.Should().BeTrue();
        }
    }
}
