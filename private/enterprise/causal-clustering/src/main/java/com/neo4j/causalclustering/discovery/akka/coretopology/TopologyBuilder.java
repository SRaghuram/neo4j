/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.stream.Collectors.toMap;

public class TopologyBuilder
{
    private final Config config;
    private final Log log;
    private final UniqueAddress uniqueAddress;

    public TopologyBuilder( Config config, UniqueAddress uniqueAddress, LogProvider logProvider )
    {
        this.config = config;
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

        boolean canBeBootstrapped = canBeBootstrapped( cluster, memberData, databaseId );
        DatabaseCoreTopology newCoreTopology = new DatabaseCoreTopology( databaseId, clusterId, canBeBootstrapped, coreMembers );
        log.debug( "Returned topology: %s", newCoreTopology );
        return newCoreTopology;
    }

    private boolean canBeBootstrapped( ClusterViewMessage cluster, MetadataMessage memberData, DatabaseId databaseId )
    {
        boolean iDoNotRefuseToBeLeader = !config.get( CausalClusteringSettings.refuse_to_be_leader );
        boolean clusterHasConverged = cluster.converged();
        boolean iAmFirstPotentialLeader = iAmFirstPotentialLeader( cluster, memberData, databaseId );

        return iDoNotRefuseToBeLeader && clusterHasConverged  && iAmFirstPotentialLeader;
    }

    private Boolean iAmFirstPotentialLeader( ClusterViewMessage cluster, MetadataMessage memberData, DatabaseId databaseId )
    {
        // Ensure consistent view of "first" member across cluster
        Optional<UniqueAddress> firstPotentialLeader = cluster.availableMembers()
                .filter( member -> potentialLeaderForDatabase( member, memberData, databaseId ) )
                .findFirst();

        return firstPotentialLeader.map( first -> first.equals( uniqueAddress ) ).orElse( false );
    }

    private boolean potentialLeaderForDatabase( UniqueAddress member, MetadataMessage memberData, DatabaseId databaseId )
    {
        return memberData.getOpt( member )
                .map( metadata -> {
                    CoreServerInfo c = metadata.coreServerInfo();
                    return !c.refusesToBeLeader() && c.getDatabaseIds().contains( databaseId );
                })
                .orElse( false );
    }
}
