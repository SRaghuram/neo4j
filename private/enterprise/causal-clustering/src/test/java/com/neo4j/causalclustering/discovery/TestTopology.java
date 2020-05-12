/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.ServerGroupName;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.causalclustering.discovery.ConnectorAddresses.ConnectorUri;
import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.bolt;
import static java.util.Collections.singletonList;

public class TestTopology
{
    private static final Set<DatabaseId> DEFAULT_DATABASE_IDS = Set.of( TestDatabaseIdRepository.randomDatabaseId() );

    private TestTopology()
    {
    }

    private static ConnectorAddresses wrapAsClientConnectorAddresses( SocketAddress advertisedSocketAddress )
    {
        return ConnectorAddresses.fromList( singletonList( new ConnectorUri( bolt, advertisedSocketAddress ) ) );
    }

    public static CoreServerInfo addressesForCore( int id, boolean refuseToBeLeader )
    {
        return addressesForCore( id, refuseToBeLeader, DEFAULT_DATABASE_IDS );
    }

    public static CoreServerInfo addressesForCore( int id, boolean refuseToBeLeader, Set<DatabaseId> databaseIds )
    {
        var raftServerAddress = new SocketAddress( "localhost", 3000 + id );
        var catchupServerAddress = new SocketAddress( "localhost", 4000 + id );
        var boltServerAddress = new SocketAddress( "localhost", 5000 + id );

        return new CoreServerInfo( raftServerAddress, catchupServerAddress, wrapAsClientConnectorAddresses( boltServerAddress ),
                ServerGroupName.setOf( "core", "core" + id ), databaseIds, refuseToBeLeader );
    }

    public static Config configFor( CoreServerInfo coreServerInfo )
    {
        return Config.newBuilder()
                .set( CausalClusteringSettings.raft_advertised_address, coreServerInfo.getRaftServer() )
                .set( CausalClusteringSettings.transaction_advertised_address, coreServerInfo.catchupServer() )
                .set( BoltConnector.listen_address, coreServerInfo.connectors().clientBoltAddress() )
                .set( BoltConnector.advertised_address, coreServerInfo.connectors().clientBoltAddress() )
                .set( BoltConnector.enabled, true )
                .set( CausalClusteringSettings.server_groups, new ArrayList<>( coreServerInfo.groups() ) )
                .set( CausalClusteringSettings.refuse_to_be_leader, coreServerInfo.refusesToBeLeader() )
                .build();
    }

    public static Config configFor( ReadReplicaInfo readReplicaInfo )
    {
        return Config.newBuilder()
                .set( BoltConnector.listen_address, readReplicaInfo.connectors().clientBoltAddress() )
                .set( BoltConnector.advertised_address, readReplicaInfo.connectors().clientBoltAddress() )
                .set( BoltConnector.enabled, true )
                .set( CausalClusteringSettings.transaction_advertised_address, readReplicaInfo.catchupServer() )
                .set( CausalClusteringSettings.server_groups, new ArrayList<>( readReplicaInfo.groups() ) )
                .build();
    }

    public static ReadReplicaInfo addressesForReadReplica( int id )
    {
        return addressesForReadReplica( id, DEFAULT_DATABASE_IDS );
    }

    public static ReadReplicaInfo addressesForReadReplica( int id, Set<DatabaseId> databaseIds )
    {
        var clientConnectorSocketAddress = new SocketAddress( "localhost", 6000 + id );
        var clientConnectorAddresses = ConnectorAddresses.fromList( List.of( new ConnectorUri( bolt, clientConnectorSocketAddress ) ) );
        var catchupSocketAddress = new SocketAddress( "localhost", 4000 + id );

        return new ReadReplicaInfo( clientConnectorAddresses, catchupSocketAddress, ServerGroupName.setOf( "replica", "replica" + id ), databaseIds );
    }

    public static Map<MemberId,ReadReplicaInfo> readReplicaInfoMap( int... ids )
    {
        return Arrays.stream( ids ).mapToObj( TestTopology::addressesForReadReplica ).collect( Collectors
                .toMap( p -> new MemberId( UUID.randomUUID() ), Function.identity() ) );
    }
}
