namespace OrleansRaft.Log
{
    using System;

    public struct LogEntryId : IEquatable<LogEntryId>
    {
        public LogEntryId(long term, long index)
        {
            this.Term = term;
            this.Index = index;
        }

        public long Term { get; }
        public long Index { get; }

        public override string ToString() => $"Term: {this.Term}, Index: {this.Index}";

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other"/> parameter; otherwise, false.
        /// </returns>
        /// <param name="other">An object to compare with this object.</param>
        public bool Equals(LogEntryId other)
        {
            return this.Term == other.Term && this.Index == other.Index;
        }

        /// <summary>
        /// Indicates whether this instance and a specified object are equal.
        /// </summary>
        /// <returns>
        /// true if <paramref name="obj"/> and this instance are the same type and represent the same value; otherwise, false. 
        /// </returns>
        /// <param name="obj">The object to compare with the current instance. </param>
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
}