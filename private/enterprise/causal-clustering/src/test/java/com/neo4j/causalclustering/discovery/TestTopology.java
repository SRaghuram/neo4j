/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.ConnectorUri;
import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static java.util.Collections.singletonList;

public class TestTopology
{
    private static final Set<DatabaseId> DEFAULT_DATABASE_IDS = Set.of( new TestDatabaseIdRepository().defaultDatabase() );

    private TestTopology()
    {
    }

    private static ClientConnectorAddresses wrapAsClientConnectorAddresses( AdvertisedSocketAddress advertisedSocketAddress )
    {
        return new ClientConnectorAddresses( singletonList( new ConnectorUri( bolt, advertisedSocketAddress ) ) );
    }

    public static CoreServerInfo addressesForCore( int id, boolean refuseToBeLeader )
    {
        return addressesForCore( id, refuseToBeLeader, DEFAULT_DATABASE_IDS );
    }

    public static CoreServerInfo addressesForCore( int id, boolean refuseToBeLeader, Set<DatabaseId> databaseIds )
    {
        var raftServerAddress = new AdvertisedSocketAddress( "localhost", 3000 + id );
        var catchupServerAddress = new AdvertisedSocketAddress( "localhost", 4000 + id );
        var boltServerAddress = new AdvertisedSocketAddress( "localhost", 5000 + id );

        return new CoreServerInfo( raftServerAddress, catchupServerAddress, wrapAsClientConnectorAddresses( boltServerAddress ),
                Set.of( "core", "core" + id ), databaseIds, refuseToBeLeader );
    }

    public static Config configFor( CoreServerInfo coreServerInfo )
    {
        return Config.builder()
                .withSetting( CausalClusteringSettings.raft_advertised_address, coreServerInfo.getRaftServer().toString() )
                .withSetting( CausalClusteringSettings.transaction_advertised_address, coreServerInfo.catchupServer().toString() )
                .withSetting( "dbms.connector.bolt.listen_address", coreServerInfo.connectors().boltAddress().toString() )
                .withSetting( "dbms.connector.bolt.enabled", String.valueOf( true ) )
                .withSetting( CausalClusteringSettings.server_groups, String.join( ",", coreServerInfo.groups() ) )
                .withSetting( CausalClusteringSettings.refuse_to_be_leader, String.valueOf( coreServerInfo.refusesToBeLeader() ) )
                .build();
    }

    public static Config configFor( ReadReplicaInfo readReplicaInfo )
    {
        return Config.builder()
                .withSetting( "dbms.connector.bolt.listen_address", readReplicaInfo.connectors().boltAddress().toString() )
                .withSetting( "dbms.connector.bolt.enabled", String.valueOf( true ) )
                .withSetting( CausalClusteringSettings.transaction_advertised_address, readReplicaInfo.catchupServer().toString() )
                .withSetting( CausalClusteringSettings.server_groups, String.join( ",", readReplicaInfo.groups() ) )
                .build();
    }

    public static ReadReplicaInfo addressesForReadReplica( int id )
    {
        return addressesForReadReplica( id, DEFAULT_DATABASE_IDS );
    }

    public static ReadReplicaInfo addressesForReadReplica( int id, Set<DatabaseId> databaseIds )
    {
        var clientConnectorSocketAddress = new AdvertisedSocketAddress( "localhost", 6000 + id );
        var clientConnectorAddresses = new ClientConnectorAddresses( List.of( new ConnectorUri( bolt, clientConnectorSocketAddress ) ) );
        var catchupSocketAddress = new AdvertisedSocketAddress( "localhost", 4000 + id );

        return new ReadReplicaInfo( clientConnectorAddresses, catchupSocketAddress, Set.of( "replica", "replica" + id ), databaseIds );
    }

    public static Map<MemberId,ReadReplicaInfo> readReplicaInfoMap( int... ids )
    {
        return Arrays.stream( ids ).mapToObj( TestTopology::addressesForReadReplica ).collect( Collectors
                .toMap( p -> new MemberId( UUID.randomUUID() ), Function.identity() ) );
    }
}
