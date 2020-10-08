/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures.wait;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.v4.info.InfoProvider;
import com.neo4j.causalclustering.discovery.TopologyService;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.Mode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.neo4j.internal.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

public class WaitProcedure extends CallableProcedure.BasicProcedure
{
    private static final String PROCEDURE_NAME = "wait";
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "admin"};
    private static final String PARAM_TX_ID = "systemTxId";
    private static final String PARAM_DATABASE_ID = "databaseId";
    private static final String PARAM_DATABASE_NAME = "databaseName";
    private static final String PARAM_TIMEOUT_S = "timeoutSeconds";

    private final Supplier<Set<ServerContext>> serverInfosProvider;
    private final ServerId myself;
    private final Clock clock;
    private final CatchupClientFactory catchupClientFactory;
    private final Log log;
    private final InfoProvider infoProvider;

    private WaitProcedure( Supplier<Set<ServerContext>> serverInfosProvider, ServerId myself, Clock clock,
            CatchupClientFactory catchupClientFactory,
            LogProvider logProvider, InfoProvider infoProvider )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .in( PARAM_TX_ID, Neo4jTypes.NTNumber )
                .in( PARAM_DATABASE_ID, Neo4jTypes.NTString )
                .in( PARAM_DATABASE_NAME, Neo4jTypes.NTString )
                .in( PARAM_TIMEOUT_S, Neo4jTypes.NTNumber )
                .out( "address", Neo4jTypes.NTString )
                .out( "state", Neo4jTypes.NTString )
                .out( "message", Neo4jTypes.NTString )
                .out( "success", Neo4jTypes.NTBoolean )
                .description( "Servers that are up to date and, if required, reconciled with the provided system transaction id." )
                .systemProcedure()
                .internal()
                .admin( true )
                .mode( Mode.DBMS )
                .build() );
        this.serverInfosProvider = serverInfosProvider;
        this.myself = myself;
        this.clock = clock;
        this.catchupClientFactory = catchupClientFactory;
        this.log = logProvider.getLog( getClass() );
        this.infoProvider = infoProvider;
    }

    public static WaitProcedure clustered( TopologyService topologyService, ServerId myself, Clock clock,
            CatchupClientFactory catchupClientFactory, LogProvider logProvider, InfoProvider infoProvider )
    {
        return new WaitProcedure(
                serverContextsProvider( topologyService ), myself, clock, catchupClientFactory, logProvider, infoProvider );
    }

    public static WaitProcedure standalone( ServerId myself, SocketAddress socketAddress, Clock clock, LogProvider logProvider,
            InfoProvider infoProvider )
    {
        return new WaitProcedure( () -> Set.of( ServerContext.local( myself, socketAddress ) ), myself, clock, null, logProvider,
                infoProvider );
    }

    private static Supplier<Set<ServerContext>> serverContextsProvider( TopologyService topologyService )
    {
        return () -> concat( topologyService.allCoreServers().entrySet().stream(), topologyService.allReadReplicas().entrySet().stream() )
                .map( info -> ServerContext.remote( info.getKey(), info.getValue() ) )
                .collect( toSet() );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        if ( input.length != 4 )
        {
            throw new IllegalArgumentException(
                    "Expected exactly 4 input parameters with input signature '" + signature().inputSignature() + "'. Got: " +
                    Arrays.stream( input ).map( Object::toString ).collect( Collectors.joining( ", ", "[", "]" ) ) );
        }
        var txId = ((LongValue) input[0]).longValue();
        var databaseId = getDatabaseId( input );
        var procedureTimeout = clock.instant().now().plus( getTimeout( input ) );
        try
        {
            var serverResponses = awaitResponses( procedureTimeout, txId, databaseId );
            var successfulOutcome = isSuccessfulOutcome( serverResponses );
            return asRawIterator( serverResponses.stream().map( serverResponse -> serverResponse.asValue( successfulOutcome ) ) );
        }
        catch ( InterruptedException e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, e, "Procedure was interrupted" );
        }
        catch ( Exception e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, e, "Procedure failed unexpectedly" );
        }
    }

    private boolean isSuccessfulOutcome( List<ServerResponse> serverResponses )
    {
        return serverResponses.stream().allMatch( serverResponse -> serverResponse.state() == WaitResponseState.CaughtUp );
    }

    private NamedDatabaseId getDatabaseId( AnyValue[] anyValue )
    {
        var databaseIdRaw = ((TextValue) anyValue[1]).stringValue();
        var databaseName = ((TextValue) anyValue[2]).stringValue();
        return DatabaseIdFactory.from( databaseName, UUID.fromString( databaseIdRaw ) );
    }

    private Duration getTimeout( AnyValue[] input )
    {
        return Duration.ofSeconds( ((LongValue) input[3]).longValue() );
    }

    private List<ServerResponse> awaitResponses( Instant procedureTimeout, long txId,
            NamedDatabaseId databaseId ) throws InterruptedException
    {
        List<ServerResponse> responses = new ArrayList<>();
        var windowedTimeout = windowedTimeout();
        var serverRequests = updateRequests( txId, databaseId, responses );
        do
        {
            for ( ServerRequest serverRequest : serverRequests )
            {
                var response = serverRequest.call();
                if ( response != null )
                {
                    responses.add( response );
                }
            }
            serverRequests = updateRequests( txId, databaseId, responses );
        }
        while ( !serverRequests.isEmpty() && poll( procedureTimeout, windowedTimeout.nextTimeout() ) );
        serverRequests.forEach( request -> responses.add( ServerResponse.incomplete( request.serverId, request.socketAddress() ) ) );
        return responses;
    }

    private WindowedTimeout windowedTimeout()
    {
        return WindowedTimeout.builder().nextWindow( 500, 20 ).nextWindow( 1000, 20 ).nextWindow( 2000, 15 ).build( 4000 );
    }

    boolean poll( Instant procedureTimeout, long timeout ) throws InterruptedException
    {
        if ( clock.instant().now().isAfter( procedureTimeout ) )
        {
            return false;
        }
        Thread.sleep( timeout );
        return true;
    }

    /**
     * Creates request for all currently known servers but filters out servers that have already responded.
     */
    private Queue<ServerRequest> updateRequests( long txId, NamedDatabaseId databaseId,
            List<ServerResponse> responses )
    {
        var completedServers = responses.stream().map( ServerResponse::serverId ).collect( Collectors.toSet() );
        Queue<ServerRequest> serverRequests = new LinkedList<>();
        serverInfosProvider.get().forEach( serverContext ->
        {
            if ( !completedServers.contains( serverContext.serverId() ) )
            {
                if ( !serverContext.serverId().equals( myself ) )
                {
                    serverRequests.add(
                            new RemoteServerRequest( txId, serverContext.serverId(), serverContext.boltAddress(), serverContext.catchupAddress(),
                                    catchupClientFactory, databaseId, log ) );
                }
                else
                {
                    serverRequests.add(
                            new LocalServerRequest( txId, serverContext.serverId(), serverContext.boltAddress(), infoProvider, databaseId,
                                    log ) );
                }
            }
        } );
        return serverRequests;
    }
}
