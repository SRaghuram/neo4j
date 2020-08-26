/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;

final class GlobalTopologyStateTestUtil
{
    private GlobalTopologyStateTestUtil()
    {
    }

    static void setupCoreTopologyState( GlobalTopologyState topologyState, NamedDatabaseId namedDatabaseId, MemberId leaderId, MemberId... followerIds )
    {
        var databaseId = namedDatabaseId.databaseId();
        var coreMembers = new HashMap<MemberId,CoreServerInfo>();

        if ( leaderId != null )
        {
            coreMembers.put( leaderId, addressesForCore( 0, false, Set.of( databaseId ) ) );
            topologyState.onDbLeaderUpdate( Map.of( databaseId, new LeaderInfo( RaftMemberId.from( leaderId ), 42 ) ) );
        }

        if ( followerIds != null )
        {
            for ( var i = 0; i < followerIds.length; i++ )
            {
                coreMembers.put( followerIds[i], addressesForCore( i + 1, false, Set.of( databaseId ) ) );
            }
        }

        var coreTopology = new DatabaseCoreTopology( databaseId, RaftId.from( databaseId ), coreMembers );
        topologyState.onTopologyUpdate( coreTopology );
    }

    static void setupReadReplicaTopologyState( GlobalTopologyState topologyState, NamedDatabaseId namedDatabaseId, MemberId... readReplicaIds )
    {
        var databaseId = namedDatabaseId.databaseId();
        var readReplicas = new HashMap<MemberId,ReadReplicaInfo>();

        if ( readReplicaIds != null )
        {
            for ( int i = 0; i < readReplicaIds.length; i++ )
            {
                readReplicas.put( readReplicaIds[i], addressesForReadReplica( i + 1, Set.of( databaseId ) ) );
            }
        }

        var readReplicaTopology = new DatabaseReadReplicaTopology( databaseId, readReplicas );
        topologyState.onTopologyUpdate( readReplicaTopology );
    }
}
