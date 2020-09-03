/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.state.machines.status.StatusRequest;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.DatabaseLogService;

public final class ClusterStatusService
{

    private final NamedDatabaseId namedDatabaseId;
    private final Lazy<RaftMemberId> myself;
    private final RaftMachine raftMachine;
    private final RaftReplicator raftReplicator;
    private final Log log;
    private final ClusterStatusResponseCollector collector;
    private final Executor executor;
    private final Duration clusterRequestMaximumWait;

    public ClusterStatusService( NamedDatabaseId namedDatabaseId, Lazy<RaftMemberId> myself, RaftMachine raftMachine,
                                 RaftReplicator raftReplicator, DatabaseLogService logService, ClusterStatusResponseCollector collector,
                                 Executor executor, Duration clusterRequestMaximumWait )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.myself = myself;
        this.raftMachine = raftMachine;
        this.raftReplicator = raftReplicator;
        this.log = logService.getInternalLog( getClass() );
        this.collector = collector;
        this.executor = executor;
        this.clusterRequestMaximumWait = clusterRequestMaximumWait;
    }

    public CompletableFuture<ClusterStatusResponse> clusterStatus( UUID requestID )
    {
        var expectedResponseCount = raftMachine.replicationMembers().size();
        collector.expectFollowerStatusesFor( requestID, expectedResponseCount );

        return CompletableFuture.supplyAsync(
                () -> raftReplicator.replicate( new StatusRequest( requestID, namedDatabaseId.databaseId(), myself.get() ) ), executor )
                .thenApply( replicationResult ->
                {
                    try
                    {
                        var responses = collector.getAllStatuses( requestID, clusterRequestMaximumWait );
                        return new ClusterStatusResponse( responses, replicationResult );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new CompletionException( e );
                    }
                } )
                .whenComplete( ( ignored, throwable ) ->
                               {
                                   if ( throwable != null )
                                   {
                                       log.warn( "Error while collecting cluster status responses for request %s: %s", requestID, throwable );
                                   }
                               } );
    }
}
