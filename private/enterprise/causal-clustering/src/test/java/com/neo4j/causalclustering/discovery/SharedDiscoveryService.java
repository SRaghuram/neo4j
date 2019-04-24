/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.unmodifiableMap;
import static org.neo4j.helpers.collection.CollectorsUtil.entriesToMap;

public final class SharedDiscoveryService
{
    private static final int MIN_DISCOVERY_MEMBERS = 2;

    private final ConcurrentMap<MemberId,CoreServerInfo> coreMembers = new ConcurrentHashMap<>();
    private final ConcurrentMap<MemberId,ReadReplicaInfo> readReplicas = new ConcurrentHashMap<>();
    private final List<SharedDiscoveryCoreClient> listeningClients = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<DatabaseId,ClusterId> clusterIdDbNames = new ConcurrentHashMap<>();
    private final ConcurrentMap<DatabaseId,LeaderInfo> leaderMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<DatabaseId,CountDownLatch> enoughMembersByDatabaseName = new ConcurrentHashMap<>();

    void waitForClusterFormation( DatabaseId databaseId ) throws InterruptedException
    {
        enoughMembersLatch( databaseId ).await();
    }

    DatabaseCoreTopology getCoreTopology( DatabaseId databaseId, SharedDiscoveryCoreClient client )
    {
        return getCoreTopology( databaseId, canBeBootstrapped( databaseId, client ) );
    }

    DatabaseCoreTopology getCoreTopology( DatabaseId databaseId, boolean canBeBootstrapped )
    {
        Map<MemberId,CoreServerInfo> databaseCoreMembers = coreMembers.entrySet()
                .stream()
                .filter( entry -> entry.getValue().getDatabaseIds().contains( databaseId ) )
                .collect( entriesToMap() );

        return new DatabaseCoreTopology( databaseId, clusterIdDbNames.get( databaseId ), canBeBootstrapped, databaseCoreMembers );
    }

    DatabaseReadReplicaTopology getReadReplicaTopology( DatabaseId databaseId )
    {
        Map<MemberId,ReadReplicaInfo> databaseReadReplicas = readReplicas.entrySet()
                .stream()
                .filter( entry -> entry.getValue().getDatabaseIds().contains( databaseId ) )
                .collect( entriesToMap() );

        return new DatabaseReadReplicaTopology( databaseId, databaseReadReplicas );
    }

    synchronized void registerCoreMember( SharedDiscoveryCoreClient client )
    {
        MemberId memberId = client.myself();
        CoreServerInfo coreServerInfo = client.getCoreServerInfo();
        CoreServerInfo previousMember = coreMembers.putIfAbsent( memberId, coreServerInfo );
        if ( previousMember == null )
        {
            listeningClients.add( client );
            for ( DatabaseId databaseId : coreServerInfo.getDatabaseIds() )
            {
                enoughMembersLatch( databaseId ).countDown();
                notifyCoreClients( databaseId );
            }
        }
    }

    synchronized void registerReadReplica( SharedDiscoveryReadReplicaClient client )
    {
        MemberId memberId = client.myself();
        ReadReplicaInfo readReplicaInfo = client.getReadReplicaInfo();
        ReadReplicaInfo previousRR = readReplicas.putIfAbsent( memberId, readReplicaInfo );
        if ( previousRR == null )
        {
            for ( DatabaseId databaseId : readReplicaInfo.getDatabaseIds() )
            {
                notifyCoreClients( databaseId );
            }
        }
    }

    synchronized void unRegisterCoreMember( SharedDiscoveryCoreClient client )
    {
        listeningClients.remove( client );
        coreMembers.remove( client.myself() );
        for ( DatabaseId databaseId : client.getDatabaseIds() )
        {
            notifyCoreClients( databaseId );
        }
    }

    synchronized void unRegisterReadReplica( SharedDiscoveryReadReplicaClient client )
    {
        readReplicas.remove( client.myself() );
        for ( DatabaseId databaseId : client.getDatabaseIds() )
        {
            notifyCoreClients( databaseId );
        }
    }

    void casLeaders( LeaderInfo newLeader, DatabaseId databaseId )
    {
        leaderMap.compute( databaseId, ( ignore, currentLeader ) ->
        {
            MemberId currentLeaderId = currentLeader != null ? currentLeader.memberId() : null;
            boolean sameLeader = Objects.equals( currentLeaderId, newLeader.memberId() );

            long currentLeaderTerm = currentLeader != null ? currentLeader.term() : -1;
            long newLeaderTerm = newLeader.term();
            boolean greaterTermExists = currentLeaderTerm > newLeaderTerm;

            boolean sameTermButNoStepDown = currentLeaderTerm == newLeaderTerm && !newLeader.isSteppingDown();

            if ( sameLeader || greaterTermExists || sameTermButNoStepDown )
            {
                return currentLeader;
            }
            else
            {
                return newLeader;
            }
        } );
    }

    boolean casClusterId( ClusterId clusterId, DatabaseId databaseId )
    {
        ClusterId previousId = clusterIdDbNames.putIfAbsent( databaseId, clusterId );

        boolean success = previousId == null || previousId.equals( clusterId );

        if ( success )
        {
            notifyCoreClients( databaseId );
        }
        return success;
    }

    Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return unmodifiableMap( coreMembers );
    }

    Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return unmodifiableMap( readReplicas );
    }

    Map<MemberId,RoleInfo> getCoreRoles()
    {
        Set<DatabaseId> dbNames = clusterIdDbNames.keySet();
        Set<MemberId> allLeaders = dbNames.stream()
                .map( dbName -> Optional.ofNullable( leaderMap.get( dbName ) ) )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .map( LeaderInfo::memberId )
                .collect( Collectors.toSet());

        Function<MemberId,RoleInfo> roleMapper = m -> allLeaders.contains( m ) ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
        return coreMembers.keySet().stream().collect( Collectors.toMap( Function.identity(), roleMapper ) );
    }

    AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
    {
        CoreServerInfo coreServerInfo = coreMembers.get( upstream );
        if ( coreServerInfo != null )
        {
            return coreServerInfo.getCatchupServer();
        }
        ReadReplicaInfo readReplicaInfo = readReplicas.get( upstream );
        if ( readReplicaInfo != null )
        {
            return readReplicaInfo.getCatchupServer();
        }
        throw new CatchupAddressResolutionException( upstream );
    }

    private synchronized void notifyCoreClients( DatabaseId databaseId )
    {
        listeningClients.forEach( client -> client.onCoreTopologyChange( getCoreTopology( databaseId, client ) ) );
    }

    private CountDownLatch enoughMembersLatch( DatabaseId databaseId )
    {
        return enoughMembersByDatabaseName.computeIfAbsent( databaseId, ignore -> new CountDownLatch( MIN_DISCOVERY_MEMBERS ) );
    }

    private boolean canBeBootstrapped( DatabaseId databaseId, SharedDiscoveryCoreClient client )
    {
        Optional<SharedDiscoveryCoreClient> firstAppropriateClient = listeningClients.stream()
                .filter( c -> !c.refusesToBeLeader() )
                .filter( c -> c.getDatabaseIds().contains( databaseId ) )
                .findFirst();

        return firstAppropriateClient.map( c -> c.equals( client ) ).orElse( false );
    }
}
