using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace Extend0.Lifecycle
{
    /// <summary>
    /// Base class that enforces exactly one live instance per concrete type within the current process.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When a derived type is constructed, the instance is registered as the sole in-process singleton
    /// for that concrete type. If another instance of the same type is created:
    /// </para>
    /// <list type="bullet">
    /// <item>
    /// <description>
    /// If <see cref="SingletonOptions.Overwrite"/> is <c>false</c>, an <see cref="InvalidOperationException"/> is thrown.
    /// </description>
    /// </item>
    /// <item>
    /// <description>
    /// If <see cref="SingletonOptions.Overwrite"/> is <c>true</c>, the previous instance is replaced and disposed.
    /// </description>
    /// </item>
    /// </list>
    /// <para>
    /// Registration and replacement are thread-safe. This class does <b>not</b> provide cross-process
    /// uniqueness by itself.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code language="csharp"><![CDATA[
    /// public sealed class MyService : Singleton
    /// {
    ///     public MyService(ILogger<MyService> logger)
    ///         : base(new SingletonOptions { Logger = logger, Overwrite = false })
    ///     {
    ///         // initialization...
    ///     }
    ///
    ///     protected override void DisposeManaged()
    ///     {
    ///         // release managed resources here
    ///     }
    /// }
    ///
    /// // Retrieving the current instance:
    /// if (Singleton.TryGet<MyService>(out var instance))
    /// {
    ///     // use instance...
    /// }
    /// ]]></code>
    /// </example>
    public class Singleton : IDisposable
    {
        private static readonly Lock _gate = new();

        /// <summary>
        /// In-process registry of the current singleton instance per concrete type.
        /// </summary>
        private static readonly ConcurrentDictionary<Type, Singleton> _byType = new();

        private bool _disposed;

        /// <summary>
        /// Initializes a new in-process singleton for the concrete derived type.
        /// Set <paramref name="options"/><see cref="SingletonOptions.Overwrite"/> to <c>true</c>
        /// to replace an existing instance.
        /// </summary>
        /// <param name="options">Behavior and logging options for singleton registration.</param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when an instance of the same concrete type already exists and
        /// <paramref name="options"/>.<see cref="SingletonOptions.Overwrite"/> is <c>false</c>.
        /// </exception>
        protected Singleton(SingletonOptions options)
        {
            var t = GetType();

            lock (_gate)
            {
                if (_byType.TryGetValue(t, out var existing))
                {
                    if (!options.Overwrite)
                        throw new InvalidOperationException(
                            $"There is already a singleton instance of type {t.FullName}.");

                    if (!ReferenceEquals(existing, this))
                    {
                        _byType[t] = this;
                        // Dispose the previous instance safely.
                        try { existing.Dispose(); }
                        catch (Exception ex)
                        {
                            options.Logger?.LogDebug(
                                "Exception produced on Singleton instance disposal: {Message}", ex.Message);
                        }
                    }
                }
                else
                {
                    _byType[t] = this;
                }
            }
        }

        /// <summary>
        /// When overridden in a derived class, releases managed resources owned by the singleton.
        /// </summary>
        protected virtual void DisposeManaged() { }

        /// <summary>
        /// Releases resources used by the singleton and unregisters it from the in-process registry.
        /// </summary>
        /// <param name="disposing">
        /// <c>true</c> to release managed resources; <c>false</c> to release unmanaged resources only.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                DisposeManaged();

                var t = GetType();
                lock (_gate)
                {
                    if (_byType.TryGetValue(t, out var cur) && ReferenceEquals(cur, this))
                        _byType.TryRemove(t, out _);
                }
            }

            _disposed = true;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Attempts to retrieve the current in-process singleton instance for <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The concrete singleton type to retrieve.</typeparam>
        /// <param name="instance">
        /// When this method returns, contains the current instance if found; otherwise <c>null</c>.
        /// </param>
        /// <returns>
        /// <c>true</c> if an instance of <typeparamref name="T"/> is registered; otherwise, <c>false</c>.
        /// </returns>
        public static bool TryGet<T>(out T? instance) where T : Singleton
        {
            if (_byType.TryGetValue(typeof(T), out var s))
            {
                instance = (T)s;
                return true;
            }
            instance = null;
            return false;
        }
    }
}
