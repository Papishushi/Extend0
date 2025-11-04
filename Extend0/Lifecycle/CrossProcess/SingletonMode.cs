namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Controls how a singleton is orchestrated: in-process only or cross-process with a single owner.
    /// </summary>
    /// <remarks>
    /// <see cref="InProcess"/> keeps one instance per process with no IPC.
    /// <see cref="CrossProcess"/> enforces a single owner process per identity and exposes IPC proxies to other processes.
    /// </remarks>
    public enum SingletonMode
    {
        /// <summary>
        /// A single instance per process; no inter-process communication involved.
        /// </summary>
        InProcess = 0,

        /// <summary>
        /// One owner process hosts the real implementation; other processes obtain an IPC proxy.
        /// </summary>
        CrossProcess = 1
    }
}
