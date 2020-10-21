/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.discovery.CoreServerInfo;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.configuration.CausalClusteringSettings.refuse_to_be_leader;
import static java.util.Objects.requireNonNull;

public class BootstrapState
{
    public static final BootstrapState EMPTY = new BootstrapState( ClusterViewMessage.EMPTY,
            MetadataMessage.EMPTY, null, null, Map.of() );

    private final ClusterViewMessage clusterView;
    private final MetadataMessage memberData;
    private final UniqueAddress selfAddress;
    private final Config config;
    private final Map<RaftId,RaftMemberId> previouslyBootstrapped;

    BootstrapState( ClusterViewMessage clusterView, MetadataMessage memberData, UniqueAddress selfAddress,
                   Config config, Map<RaftId,RaftMemberId> previouslyBootstrapped )
    {
        this.clusterView = requireNonNull( clusterView );
        this.memberData = requireNonNull( memberData );
        this.selfAddress = selfAddress;
        this.config = config;
        this.previouslyBootstrapped = previouslyBootstrapped;
    }

    public boolean canBootstrapRaft( NamedDatabaseId namedDatabaseId )
    {
        boolean iDoNotRefuseToBeLeader = config != null && !config.get( refuse_to_be_leader );
        boolean clusterHasConverged = clusterView.converged();
        boolean iAmFirstPotentialLeader = iAmFirstPotentialLeader( namedDatabaseId );

        return iDoNotRefuseToBeLeader && clusterHasConverged && iAmFirstPotentialLeader;
    }

    public boolean memberBootstrappedRaft( NamedDatabaseId namedDatabaseId, RaftMemberId raftMemberId )
    {
        var raftid = RaftId.from( namedDatabaseId.databaseId() );
        var bootstrapper = previouslyBootstrapped.get( raftid );
        return Objects.equals( raftMemberId, bootstrapper );
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
               Objects.equals( config, that.config ) &&
               Objects.equals( previouslyBootstrapped, that.previouslyBootstrapped );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( clusterView, memberData, selfAddress, config, previouslyBootstrapped );
    }

    @Override
    public String toString()
    {
        return "BootstrapState{" +
                "clusterView=" + clusterView +
                ", memberData=" + memberData +
                ", selfAddress=" + selfAddress +
                ", previouslyBootstrapped=" + previouslyBootstrapped +
                '}';
    }

    private boolean iAmFirstPotentialLeader( NamedDatabaseId namedDatabaseId )
    {
        // Ensure consistent view of "first" member across cluster
        Optional<UniqueAddress> firstPotentialLeader = clusterView.availableMembers()
                .filter( member -> isPotentialLeader( member, namedDatabaseId ) )
                .findFirst();

        return firstPotentialLeader.map( address -> Objects.equals( address, selfAddress ) ).orElse( false );
    }

    private boolean isPotentialLeader( UniqueAddress member, NamedDatabaseId namedDatabaseId )
    {
        return memberData.getOpt( member )
                .map( metadata -> isPotentialLeader( metadata, namedDatabaseId ) )
                .orElse( false );
    }

    private static boolean isPotentialLeader( CoreServerInfoForServerId infoForMember, NamedDatabaseId namedDatabaseId )
    {
        CoreServerInfo info = infoForMember.coreServerInfo();
        return !info.refusesToBeLeader() && info.startedDatabaseIds().contains( namedDatabaseId.databaseId() );
    }
}
