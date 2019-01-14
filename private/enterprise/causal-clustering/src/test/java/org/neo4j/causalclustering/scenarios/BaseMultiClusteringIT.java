/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.helpers.DataCreator;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.causalclustering.ClusterConfig;
import org.neo4j.test.causalclustering.ClusterExtension;
import org.neo4j.test.causalclustering.ClusterFactory;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.causalclustering.TestStoreId.getStoreIds;
import static org.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static org.neo4j.graphdb.Label.label;

@ClusterExtension
@ExtendWith( DefaultFileSystemExtension.class )
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
public abstract class BaseMultiClusteringIT
{
    protected static Set<String> DB_NAMES_1 = Collections.singleton( "default" );
    protected static Set<String> DB_NAMES_2 = Stream.of( "foo", "bar" ).collect( Collectors.toSet() );
    protected static Set<String> DB_NAMES_3 = Stream.of( "foo", "bar", "baz" ).collect( Collectors.toSet() );

    private final Set<String> dbNames;
    private final ClusterConfig clusterConfig;

    private Cluster<?> cluster;
    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private ClusterFactory clusterFactory;

    protected BaseMultiClusteringIT( int numCores, int numReplicas, Set<String> dbNames, DiscoveryServiceType discoveryServiceType )
    {
        this.dbNames = dbNames;

        this.clusterConfig = ClusterConfig
                .clusterConfig()
                .withNumberOfCoreMembers( numCores )
                .withNumberOfReadReplicas( numReplicas )
                .withDatabaseNames( dbNames )
                .withDiscoveryServiceType( discoveryServiceType );
    }

    @BeforeEach
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldRunDistinctTransactionsAndDiverge() throws Exception
    {
        int numNodes = 1;
        Map<CoreClusterMember, List<CoreClusterMember>> leaderMap = new HashMap<>();
        for ( String dbName : dbNames )
        {
            int i = 0;
            CoreClusterMember leader;

            do
            {
                leader = cluster.coreTx( dbName, ( db, tx ) ->
                {
                    Node node = db.createNode( label("database") );
                    node.setProperty( "name" , dbName );
                    tx.success();
                } );
                i++;
            }
            while ( i < numNodes );

            int leaderId = leader.serverId();
            List<CoreClusterMember> notLeaders = cluster.coreMembers().stream()
                    .filter( m -> m.dbName().equals( dbName ) && m.serverId() != leaderId )
                    .collect( Collectors.toList() );

            leaderMap.put( leader, notLeaders );
            numNodes++;
        }

        Set<Long> nodesPerDb = leaderMap.keySet().stream()
                .map( DataCreator::countNodes ).collect( Collectors.toSet() );
        assertEquals( nodesPerDb.size(), dbNames.size(), "Each logical database in the multicluster should have a unique number of nodes." );
        for ( Map.Entry<CoreClusterMember, List<CoreClusterMember>> subCluster : leaderMap.entrySet() )
        {
            dataMatchesEventually( subCluster.getKey(), subCluster.getValue() );
        }

    }

    @Test
    void distinctDatabasesShouldHaveDistinctStoreIds() throws Exception
    {
        for ( String dbName : dbNames )
        {
            cluster.coreTx( dbName, ( db, tx ) ->
            {
                Node node = db.createNode( label("database") );
                node.setProperty( "name" , dbName );
                tx.success();
            } );
        }

        List<File> storeDirs = cluster.coreMembers().stream()
                .map( CoreClusterMember::databaseDirectory )
                .collect( Collectors.toList() );

        cluster.shutdown();

        Set<StoreId> storeIds = getStoreIds( storeDirs, fs );
        int expectedNumStoreIds = dbNames.size();
        assertEquals( expectedNumStoreIds, storeIds.size(), "Expected distinct store ids for distinct sub clusters." );
    }

    @Test
    void rejoiningFollowerShouldDownloadSnapshotFromCorrectDatabase() throws Exception
    {
        String dbName = getFirstDbName( dbNames );
        int followerId = cluster.getMemberWithAnyRole( dbName, Role.FOLLOWER ).serverId();
        cluster.removeCoreMemberWithServerId( followerId );

        for ( int  i = 0; i < 100; i++ )
        {
            cluster.coreTx( dbName, ( db, tx ) ->
            {
                Node node = db.createNode( label( dbName + "Node" ) );
                node.setProperty( "name", dbName );
                tx.success();
            } );
        }

        for ( CoreClusterMember m : cluster.coreMembers() )
        {
            m.raftLogPruner().prune();
        }

        cluster.addCoreMemberWithId( followerId ).start();

        CoreClusterMember dbLeader = cluster.awaitLeader( dbName );

        boolean followerIsHealthy = cluster.healthyCoreMembers().stream()
                .anyMatch( m -> m.serverId() == followerId );

        assertTrue( followerIsHealthy, "Rejoining / lagging follower is expected to be healthy." );

        CoreClusterMember follower = cluster.getCoreMemberById( followerId );

        dataMatchesEventually( dbLeader, Collections.singleton(follower) );

        List<File> storeDirs = cluster.coreMembers().stream()
                .filter( m -> dbName.equals( m.dbName() ) )
                .map( CoreClusterMember::databaseDirectory )
                .collect( Collectors.toList() );

        cluster.shutdown();

        Set<StoreId> storeIds = getStoreIds( storeDirs, fs );
        String message = "All members of a sub-cluster should have the same store Id after downloading a snapshot.";
        assertEquals( 1, storeIds.size(), message );
    }

    @Test
    void shouldNotBeAbleToChangeClusterMembersDatabaseName()
    {
        CoreClusterMember member = cluster.coreMembers().stream().findFirst().orElseThrow( IllegalArgumentException::new );

        member.shutdown();

        //given
        member.updateConfig( CausalClusteringSettings.database, "new_name" );

        // when then
        assertThrows( RuntimeException.class, member::start );
    }

    //TODO: Test that rejoining followers wait for majority of hosts *for each database* to be available before joining

    private static String getFirstDbName( Set<String> dbNames )
    {
        return dbNames.stream()
                .findFirst()
                .orElseThrow( () -> new IllegalArgumentException( "The dbNames parameter must not be empty." ) );
    }
}
