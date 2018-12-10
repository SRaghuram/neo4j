/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

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

    CoreTopology buildCoreTopology( @Nullable ClusterId clusterId, ClusterViewMessage cluster, MetadataMessage memberData )
    {

        log.debug( "Building new view of Topology from actor %s, cluster state is: %s, metadata is %s", uniqueAddress, cluster, memberData );
        Map<MemberId, CoreServerInfo> coreMembers =
                getCoreInfos( cluster, memberData )
                .collect( Collectors.toMap( CoreServerInfoForMemberId::memberId, CoreServerInfoForMemberId::coreServerInfo ) );

        boolean canBeBootstrapped = canBeBootstrapped( cluster, memberData );
        CoreTopology newCoreTopology = new CoreTopology( clusterId, canBeBootstrapped, coreMembers );
        log.debug( "Returned topology: %s", newCoreTopology );
        return newCoreTopology;
    }

    private Stream<CoreServerInfoForMemberId> getCoreInfos( ClusterViewMessage cluster, MetadataMessage memberData )
    {
         return cluster
                 .availableMembers()
                 .flatMap( memberData::getStream );
    }

    private boolean canBeBootstrapped( ClusterViewMessage cluster, MetadataMessage memberData )
    {
        boolean iDoNotRefuseToBeLeader = !config.get( CausalClusteringSettings.refuse_to_be_leader );
        boolean clusterHasConverged = cluster.converged();
        String dbName = config.get( CausalClusteringSettings.database );
        boolean iAmFirstPotentialLeader = iAmFirstPotentialLeader( cluster, memberData, dbName );

        return iDoNotRefuseToBeLeader && clusterHasConverged  && iAmFirstPotentialLeader;
    }

    private Boolean iAmFirstPotentialLeader( ClusterViewMessage cluster, MetadataMessage memberData, String dbName )
    {
        // Ensure consistent view of "first" member across cluster
        Optional<UniqueAddress> firstPotentialLeader = cluster.availableMembers()
                .filter( member -> potentialLeaderForDatabase( member, memberData, dbName ) )
                .findFirst();

        return firstPotentialLeader.map( first -> first.equals( uniqueAddress ) ).orElse( false );
    }

    private boolean potentialLeaderForDatabase( UniqueAddress member, MetadataMessage memberData, String dbName )
    {
        return memberData.getOpt( member )
                .map( metadata -> {
                    CoreServerInfo c = metadata.coreServerInfo();
                    return !c.refusesToBeLeader() && c.getDatabaseName().equals( dbName );
                })
                .orElse( false );
    }
}
