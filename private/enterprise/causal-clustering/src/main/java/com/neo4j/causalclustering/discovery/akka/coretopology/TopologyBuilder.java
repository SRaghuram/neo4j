/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import javax.annotation.Nullable;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.stream.Collectors.toMap;

public class TopologyBuilder
{
    private final Log log;
    private final UniqueAddress uniqueAddress;

    public TopologyBuilder( UniqueAddress uniqueAddress, LogProvider logProvider )
    {
        this.uniqueAddress = uniqueAddress;
        this.log = logProvider.getLog( getClass() );
    }

    DatabaseCoreTopology buildCoreTopology( DatabaseId databaseId, @Nullable ClusterId clusterId, ClusterViewMessage cluster, MetadataMessage memberData )
    {
        log.debug( "Building new view of core topology from actor %s, cluster state is: %s, metadata is %s", uniqueAddress, cluster, memberData );
        Map<MemberId,CoreServerInfo> coreMembers = cluster.availableMembers()
                .flatMap( memberData::getStream )
                .filter( member -> member.coreServerInfo().getDatabaseIds().contains( databaseId ) )
                .collect( toMap( CoreServerInfoForMemberId::memberId, CoreServerInfoForMemberId::coreServerInfo ) );

        DatabaseCoreTopology newCoreTopology = new DatabaseCoreTopology( databaseId, clusterId, coreMembers );
        log.debug( "Returned topology: %s", newCoreTopology );
        return newCoreTopology;
    }
}
