namespace Orleans.Consensus.UnitTests.Utilities
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;

    using Autofac;

    internal class FakeGrainFactory : IGrainFactory
    {
        private readonly ConcurrentDictionary<Tuple<Type, Guid>, IGrainWithGuidKey> grainWithGuidKeys =
            new ConcurrentDictionary<Tuple<Type, Guid>, IGrainWithGuidKey>();

        private readonly ConcurrentDictionary<Tuple<Type, long>, IGrainWithIntegerKey> grainWithIntegerKeys =
            new ConcurrentDictionary<Tuple<Type, long>, IGrainWithIntegerKey>();

        private readonly ConcurrentDictionary<Tuple<Type, string>, IGrainWithStringKey> grainWithStringKeys =
            new ConcurrentDictionary<Tuple<Type, string>, IGrainWithStringKey>();

        private readonly ConcurrentDictionary<Tuple<Type, Guid, string>, IGrainWithGuidCompoundKey>
            grainWithGuidCompoundKeys = new ConcurrentDictionary<Tuple<Type, Guid, string>, IGrainWithGuidCompoundKey>();

        private readonly ConcurrentDictionary<Tuple<Type, long, string>, IGrainWithIntegerCompoundKey>
            grainWithIntegerCompoundKeys =
                new ConcurrentDictionary<Tuple<Type, long, string>, IGrainWithIntegerCompoundKey>();

        private readonly ConcurrentDictionary<Tuple<Type, int>, IGrainObserver> objectReferences =
            new ConcurrentDictionary<Tuple<Type, int>, IGrainObserver>();

        private readonly IComponentContext container;

        public FakeGrainFactory(IComponentContext container)
        {
            this.container = container.Resolve<IComponentContext>();
        }

        public Action<object, IGrain> OnGrainCreated { get; set; } = (_, __) => { };

        public TGrainInterface GetGrain<TGrainInterface>(Guid primaryKey, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithGuidKey
        {
            return
                (TGrainInterface)
                this.grainWithGuidKeys.GetOrAdd(
                    Tuple.Create(typeof(TGrainInterface), primaryKey),
                    _ => this.CreateGrain<TGrainInterface>(primaryKey));
        }

        private TGrainInterface CreateGrain<TGrainInterface>(object primaryKey) where TGrainInterface : IGrain
        {
            var result = (TGrainInterface)this.container.Resolve(typeof(TGrainInterface));
            this.OnGrainCreated?.Invoke(primaryKey, result);
            return result;
        }

        public TGrainInterface GetGrain<TGrainInterface>(long primaryKey, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithIntegerKey
        {
            return
                (TGrainInterface)
                this.grainWithIntegerKeys.GetOrAdd(
                    Tuple.Create(typeof(TGrainInterface), primaryKey),
                    _ => this.CreateGrain<TGrainInterface>(primaryKey));
        }

        public TGrainInterface GetGrain<TGrainInterface>(string primaryKey, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithStringKey
        {
            return
                (TGrainInterface)
                this.grainWithStringKeys.GetOrAdd(
                    Tuple.Create(typeof(TGrainInterface), primaryKey),
                    _ => this.CreateGrain<TGrainInterface>(primaryKey));
        }

        public TGrainInterface GetGrain<TGrainInterface>(
            long primaryKey,
            string keyExtension,
            string grainClassNamePrefix = null) where TGrainInterface : IGrainWithIntegerCompoundKey
        {
            return
                (TGrainInterface)
                this.grainWithIntegerCompoundKeys.GetOrAdd(
                    Tuple.Create(typeof(TGrainInterface), primaryKey, keyExtension),
                    _ => this.CreateGrain<TGrainInterface>(Tuple.Create(primaryKey, keyExtension)));
        }

        public Task<TGrainObserverInterface> CreateObjectReference<TGrainObserverInterface>(IGrainObserver obj)
            where TGrainObserverInterface : IGrainObserver
        {
            return
                Task.FromResult(
                    (TGrainObserverInterface)
                    this.objectReferences.GetOrAdd(
                        Tuple.Create(typeof(TGrainObserverInterface), obj.GetHashCode()),
                        _ => obj));
        }

        public Task DeleteObjectReference<TGrainObserverInterface>(IGrainObserver obj)
            where TGrainObserverInterface : IGrainObserver
        {
            this.objectReferences.TryRemove(Tuple.Create(typeof(TGrainObserverInterface), obj.GetHashCode()), out obj);
            return Task.FromResult(0);
        }

        public TGrainInterface GetGrain<TGrainInterface>(
            Guid primaryKey,
            string keyExtension,
            string grainClassNamePrefix = null) where TGrainInterface : IGrainWithGuidCompoundKey
        {
            return
                (TGrainInterface)
                this.grainWithGuidCompoundKeys.GetOrAdd(
                    Tuple.Create(typeof(TGrainInterface), primaryKey, keyExtension),
                    _ => this.CreateGrain<TGrainInterface>(Tuple.Create(primaryKey, keyExtension)));
        }
    }
}