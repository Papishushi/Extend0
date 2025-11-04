namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Exception thrown when a remote method invocation fails on the server side
    /// or when the IPC layer returns an error envelope.
    /// </summary>
    /// <remarks>
    /// Raised by the RPC client when the response payload is <c>{"ok": false, "e": "..."} </c>
    /// or when a protocol-level error is detected. The message typically contains the
    /// error text emitted by the remote endpoint.
    /// </remarks>
    /// <param name="message">
    /// The error message returned by the remote endpoint or a local protocol error description.
    /// </param>
    internal sealed class RemoteInvocationException(string message) : Exception(message)
    {
    }
}
