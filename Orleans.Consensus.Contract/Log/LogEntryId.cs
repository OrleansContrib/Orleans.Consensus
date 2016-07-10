namespace Orleans.Consensus.Contract.Log
{
    using System;

    using Orleans.Concurrency;

    [Immutable]
    public struct LogEntryId : IEquatable<LogEntryId>, IComparable<LogEntryId>
    {
        public LogEntryId(long term, long index)
        {
            if (term < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(term), "Term must be >= zero.");
            }

            if (index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index), "Index must be >= zero.");
            }

            this.Term = term;
            this.Index = index;
        }

        public long Term { get; }

        public long Index { get; }

        /// <summary>
        /// Compares the current object with another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// A value that indicates the relative order of the objects being compared. The return value has the following
        /// meanings: Value Meaning Less than zero This object is less than the <paramref name="other"/> parameter.
        /// Zero This object is equal to <paramref name="other"/>. Greater than zero This object is greater than
        /// <paramref name="other"/>. 
        /// </returns>
        public int CompareTo(LogEntryId other)
        {
            if (this.Term > other.Term)
            {
                return 1;
            }

            if (this.Term < other.Term)
            {
                return -1;
            }

            if (this.Index > other.Index)
            {
                return 1;
            }

            if (this.Index < other.Index)
            {
                return -1;
            }

            return 0;
        }

        public override string ToString() => $"{this.Term}.{this.Index}";

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other"/> parameter; otherwise, false.
        /// </returns>
        public bool Equals(LogEntryId other)
        {
            return this.Term == other.Term && this.Index == other.Index;
        }

        /// <summary>
        /// Indicates whether this instance and a specified object are equal.
        /// </summary>
        /// <param name="obj">The object to compare with the current instance. </param>
        /// <returns>
        /// true if <paramref name="obj"/> and this instance are the same type and represent the same value; otherwise, false. 
        /// </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            return obj is LogEntryId && this.Equals((LogEntryId)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>
        /// A 32-bit signed integer that is the hash code for this instance.
        /// </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                return (this.Term.GetHashCode() * 397) ^ this.Index.GetHashCode();
            }
        }

        public static bool operator ==(LogEntryId left, LogEntryId right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(LogEntryId left, LogEntryId right)
        {
            return !left.Equals(right);
        }

        public static bool operator >(LogEntryId left, LogEntryId right)
        {
            if (left.Term > right.Term)
            {
                return true;
            }

            if (left.Term < right.Term)
            {
                return false;
            }

            return left.Index > right.Index;
        }

        public static bool operator <(LogEntryId left, LogEntryId right)
        {
            if (left.Term < right.Term)
            {
                return true;
            }

            if (left.Term > right.Term)
            {
                return false;
            }

            return left.Index < right.Index;
        }

        public static bool operator <=(LogEntryId left, LogEntryId right)
        {
            return left < right || left == right;
        }

        public static bool operator >=(LogEntryId left, LogEntryId right)
        {
            return left > right || left == right;
        }
    }

    /// <summary>
    /// A mutable version of <see cref="LogEntryId"/>, for use by the ProtoBuf serializer
    /// </summary>
    public struct MutableLogEntryId
    {
        public static implicit operator LogEntryId(MutableLogEntryId surrogate)
        {
            return new LogEntryId(surrogate.Term, surrogate.Index);
        }

        public static implicit operator MutableLogEntryId(LogEntryId surrogate)
        {
            return new MutableLogEntryId { Term = surrogate.Term, Index = surrogate.Index };
        }

        public long Term { get; set; }

        public long Index { get; set; }
    }
}