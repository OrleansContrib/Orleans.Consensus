namespace Orleans.Consensus.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class MockTimers
    {
        public IDisposable RegisterTimer(Func<object, Task> callback, object state, TimeSpan dueTime, TimeSpan period)
        {
            var registration = new TimerRegistration(callback, state, dueTime, period);
            this.Registrations.Add(registration);
            return registration.Disposable;
        }
        
        public readonly List<TimerRegistration> Registrations = new List<TimerRegistration>(); 

        public class TimerRegistration
        {
            public TimerRegistration(Func<object, Task> callback, object state, TimeSpan dueTime, TimeSpan period)
            {
                this.Callback = callback;
                this.State = state;
                this.DueTime = dueTime;
                this.Period = period;
            }

            public Disposable Disposable { get; } = new Disposable();
            public Func<object, Task> Callback { get; }

            public object State { get; }
            public TimeSpan DueTime { get; }
            public TimeSpan Period { get; }
        }
    }
}