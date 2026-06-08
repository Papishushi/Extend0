namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Minimal lifetime contract for an owner-side cross-process host.
    /// </summary>
    public interface ICrossProcessServerHost : IDisposable, IAsyncDisposable
    {
    }
}
