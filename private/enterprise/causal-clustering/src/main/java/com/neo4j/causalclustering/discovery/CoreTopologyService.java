/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.identity.RaftId;

import org.neo4j.kernel.database.DatabaseId;

/**
 * Extends upon the topology service with a few extra services, connected to
 * the underlying discovery service.
 */
public interface CoreTopologyService extends TopologyService
{
    void addLocalCoreTopologyListener( Listener listener );

    void removeLocalCoreTopologyListener( Listener listener );

    /**
     * Publishes the raft ID so that other members might discover it.
     * Should only succeed to publish if one missing or already the same (CAS logic).
     *
     * @param raftId The Raft ID to publish.
     * @return True if the raft ID was successfully CAS:ed, otherwise false.
     */
    boolean setRaftId( RaftId raftId ) throws InterruptedException;

    /**
     * Sets or updates the leader memberId for the given database (i.e. Raft consensus group).
     * This is intended for informational purposes **only**, e.g. in {@link ClusterOverviewProcedure}.
     * The leadership information should otherwise be communicated via raft as before.
     *
     * @param leaderInfo Information about the new leader
     * @param databaseId The database identifier for which memberId is the new leader
     */
    void setLeader( LeaderInfo leaderInfo, DatabaseId databaseId );

    /**
     * Set the leader memberId to null for a given database (i.e. Raft consensus group).
     * This is intended to trigger state cleanup for informational procedures like {@link ClusterOverviewProcedure}
     *
     * @param term The term for which this topology member should handle a step-down.
     * @param databaseId The database for which this topology member should handle a step-down.
     */
    void handleStepDown( long term, DatabaseId databaseId );

    /**
     * Check if this cluster member can bootstrap the Raft group for the specified database.
     *
     * @param databaseId the database to bootstrap.
     * @return {@code true} if this cluster member can bootstrap, {@code false} otherwise.
     */
    boolean canBootstrapRaftGroup( DatabaseId databaseId );

    interface Listener
    {
        void onCoreTopologyChange( DatabaseCoreTopology coreTopology );

        DatabaseId databaseId();
    }
}
