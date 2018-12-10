/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.identity.ClusterId;

/**
 * Extends upon the topology service with a few extra services, connected to
 * the underlying discovery service.
 */
public interface CoreTopologyService extends TopologyService
{
    void addLocalCoreTopologyListener( Listener listener );

    void removeLocalCoreTopologyListener( Listener listener );

    /**
     * Publishes the cluster ID so that other members might discover it.
     * Should only succeed to publish if one missing or already the same (CAS logic).
     *
     * @param clusterId The cluster ID to publish.
     *
     * @return True if the cluster ID was successfully CAS:ed, otherwise false.
     */
    boolean setClusterId( ClusterId clusterId, String dbName ) throws InterruptedException;

    /**
     * Sets or updates the leader memberId for the given database (i.e. Raft consensus group).
     * This is intended for informational purposes **only**, e.g. in {@link ClusterOverviewProcedure}.
     * The leadership information should otherwise be communicated via raft as before.
     * @param leaderInfo Information about the new leader
     * @param dbName The database name for which memberId is the new leader
     */
    void setLeader( LeaderInfo leaderInfo, String dbName );

    /** Fetches info for the current leader */
    LeaderInfo getLeader();

    /**
     * Set the leader memberId to null for a given database (i.e. Raft consensus group).
     * This is intended to trigger state cleanup for informational procedures like {@link ClusterOverviewProcedure}
     *
     * @param term The term for which this topology member should handle a stepdown
     * @param dbName The database for which this topology member should handle a stepdown
     */
    void handleStepDown( long term, String dbName );

    interface Listener
    {
        void onCoreTopologyChange( CoreTopology coreTopology );
        String dbName();
    }
}
