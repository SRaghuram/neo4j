/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;

import java.util.Comparator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.values.AnyValue;

import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;

public class InstalledProtocolsProcedure extends CallableProcedure.BasicProcedure
{
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "cluster"};

    public static final String PROCEDURE_NAME = "protocols";

    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> clientInstalledProtocols;
    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> serverInstalledProtocols;

    public InstalledProtocolsProcedure( Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> clientInstalledProtocols,
            Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> serverInstalledProtocols )
    {
        super( ProcedureSignature.procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .out( "orientation", Neo4jTypes.NTString )
                .out( "remoteAddress", Neo4jTypes.NTString )
                .out( "applicationProtocol", Neo4jTypes.NTString )
                .out( "applicationProtocolVersion", Neo4jTypes.NTInteger )
                .out( "modifierProtocols", Neo4jTypes.NTString )
                .description( "Overview of installed protocols" )
                .systemProcedure()
                .build() );
        this.clientInstalledProtocols = clientInstalledProtocols;
        this.serverInstalledProtocols = serverInstalledProtocols;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply(
            Context ctx, AnyValue[] input, ResourceTracker resourceTracker )
    {
        Stream<AnyValue[]> outbound = toOutputRows( clientInstalledProtocols, ProtocolInstaller.Orientation.Client.OUTBOUND );

        Stream<AnyValue[]> inbound = toOutputRows( serverInstalledProtocols, ProtocolInstaller.Orientation.Server.INBOUND );

        return Iterators.asRawIterator( Stream.concat( outbound, inbound ) );
    }

    private <T extends SocketAddress> Stream<AnyValue[]> toOutputRows( Supplier<Stream<Pair<T,ProtocolStack>>> installedProtocols, String orientation )
    {
        Comparator<Pair<T,ProtocolStack>> connectionInfoComparator = Comparator.comparing( ( Pair<T,ProtocolStack> entry ) -> entry.first().getHostname() )
                .thenComparing( entry -> entry.first().getPort() );

        return installedProtocols.get()
                .sorted( connectionInfoComparator )
                .map( entry -> buildRow( entry, orientation ) );
    }

    private <T extends SocketAddress> AnyValue[] buildRow( Pair<T,ProtocolStack> connectionInfo, String orientation )
    {
        T socketAddress = connectionInfo.first();
        ProtocolStack protocolStack = connectionInfo.other();
        return new AnyValue[]
                {
                        utf8Value( orientation ),
                        utf8Value( socketAddress.toString() ),
                        utf8Value( protocolStack.applicationProtocol().category() ),
                        utf8Value( protocolStack.applicationProtocol().implementation().toString() ),
                        utf8Value( modifierString( protocolStack ) )
                };
    }

    private String modifierString( ProtocolStack protocolStack )
    {
        return protocolStack
                .modifierProtocols()
                .stream()
                .map( ModifierProtocol::implementation )
                .collect( Collectors.joining( ",", "[", "]") );
    }
}
