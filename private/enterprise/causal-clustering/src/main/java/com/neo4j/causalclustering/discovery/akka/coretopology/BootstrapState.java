/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.discovery.CoreServerInfo;

import java.util.Objects;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.refuse_to_be_leader;
import static java.util.Objects.requireNonNull;

public class BootstrapState
{
    public static final BootstrapState EMPTY = new BootstrapState( ClusterViewMessage.EMPTY, MetadataMessage.EMPTY, null, null );

    private final ClusterViewMessage clusterView;
    private final MetadataMessage memberData;
    private final UniqueAddress selfAddress;
    private final Config config;

    BootstrapState( ClusterViewMessage clusterView, MetadataMessage memberData, UniqueAddress selfAddress, Config config )
    {
        this.clusterView = requireNonNull( clusterView );
        this.memberData = requireNonNull( memberData );
        this.selfAddress = selfAddress;
        this.config = config;
    }

    public boolean canBootstrapRaft( DatabaseId databaseId )
    {
        boolean iDoNotRefuseToBeLeader = config != null && !config.get( refuse_to_be_leader );
        boolean clusterHasConverged = clusterView.converged();
        boolean iAmFirstPotentialLeader = iAmFirstPotentialLeader( databaseId );

        return iDoNotRefuseToBeLeader && clusterHasConverged && iAmFirstPotentialLeader;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        BootstrapState that = (BootstrapState) o;
        return Objects.equals( clusterView, that.clusterView ) &&
               Objects.equals( memberData, that.memberData ) &&
               Objects.equals( selfAddress, that.selfAddress ) &&
               Objects.equals( config, that.config );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( clusterView, memberData, selfAddress, config );
    }

    private boolean iAmFirstPotentialLeader( DatabaseId databaseId )
    {
        // Ensure consistent view of "first" member across cluster
        Optional<UniqueAddress> firstPotentialLeader = clusterView.availableMembers()
                .filter( member -> isPotentialLeader( member, databaseId ) )
                .findFirst();

        return firstPotentialLeader.map( address -> Objects.equals( address, selfAddress ) ).orElse( false );
    }

    private boolean isPotentialLeader( UniqueAddress member, DatabaseId databaseId )
    {
        return memberData.getOpt( member )
                .map( metadata -> isPotentialLeader( metadata, databaseId ) )
                .orElse( false );
    }

    private static boolean isPotentialLeader( CoreServerInfoForMemberId infoForMember, DatabaseId databaseId )
    {
        CoreServerInfo info = infoForMember.coreServerInfo();
        return !info.refusesToBeLeader() && info.databaseIds().contains( databaseId );
    }
}
