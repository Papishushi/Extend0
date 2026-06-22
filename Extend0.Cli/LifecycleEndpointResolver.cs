using Extend0.Lifecycle.CrossProcess;
using Extend0.Metadata.CrossProcess.Contract;

namespace Extend0.Cli;

internal static class LifecycleEndpointResolver
{
    public static string ResolveEndpointName(LifecycleProbeOptions options) =>
        options.ContractKind switch
        {
            LifecycleContractKind.MetaDB => CrossProcessTransportFactory.ResolveEndpointNameFor<IMetaDBManagerRPCCompatible>(
                options.Name,
                options.TransportKind,
                options.EndpointName,
                allowLogicalFallback: options.AllowCustom),
            _ => CrossProcessTransportFactory.ResolveEndpointName(
                options.Name,
                options.TransportKind,
                options.EndpointName,
                allowLogicalFallback: options.AllowCustom)
        };
}
