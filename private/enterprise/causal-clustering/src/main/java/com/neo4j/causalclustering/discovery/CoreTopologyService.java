/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Extends upon the topology service with a few extra services, connected to
 * the underlying discovery service.
 */
public interface CoreTopologyService extends TopologyService
{

    SocketAddress lookupRaftAddress( RaftMemberId target );

    void addLocalCoreTopologyListener( Listener listener );

    void removeLocalCoreTopologyListener( Listener listener );

    /**
     * Publishes the raft ID as a signal to other cluster members that this raft group bas been bootstrapped.
     * Raft groups should have a single bootstrapper, so this operation should only succeed if the given
     * raft Id is missing from the discovery service's shared state, or has been previously published by
     * this same cluster member.
     *
     * @param raftGroupId The Raft ID to publish.
     * @param memberId The Member ID of the member in the RaftGroup which is publishing.
     * @return The outcome of this publish attempt
     * @throws TimeoutException if request retries. This means that the outcome is unknown
     */
    PublishRaftIdOutcome publishRaftId( RaftGroupId raftGroupId, RaftMemberId memberId ) throws TimeoutException;

    /**
     * Sets or updates the leader memberId for the given database (i.e. Raft consensus group).
     * This is intended for informational purposes **only**, e.g. in {@link ClusterOverviewProcedure}.
     * The leadership information should otherwise be communicated via raft as before.
     *
     * @param leaderInfo Information about the new leader
     * @param namedDatabaseId The database identifier for which memberId is the new leader
     */
    void setLeader( LeaderInfo leaderInfo, NamedDatabaseId namedDatabaseId );

    /**
     * Set the leader memberId to null for a given database (i.e. Raft consensus group).
     * This is intended to trigger state cleanup for informational procedures like {@link ClusterOverviewProcedure}
     *
     * @param term The term for which this topology member should handle a step-down.
     * @param namedDatabaseId The database for which this topology member should handle a step-down.
     */
    void handleStepDown( long term, NamedDatabaseId namedDatabaseId );

    /**
     * Check if this cluster member can bootstrap the Raft group for the specified database.
     *
     * @param namedDatabaseId the database to bootstrap.
     * @return {@code true} if this cluster member can bootstrap, {@code false} otherwise.
     */
    boolean canBootstrapDatabase( NamedDatabaseId namedDatabaseId );

    /**
     * Check if this cluster member *did* bootstrap the Raft group for the specified database.
     *
     * @param namedDatabaseId the database that was bootstrapped
     * @return {@code true} if this cluster member bootstrapped the given database, {@code false} otherwise
     */
    boolean didBootstrapDatabase( NamedDatabaseId namedDatabaseId );

    interface Listener
    {
        void onCoreTopologyChange( Set<RaftMemberId> memberIds );

        NamedDatabaseId namedDatabaseId();
    }
}
