/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.dbms.ShowDatabasesHelpers.ShowDatabasesResultRow;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.dbms.ShowDatabasesHelpers;
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Nested;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.dbms.OperatorState;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.dropDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.showDatabases;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

class ClusteredShowDatabasesIT
{

    private static String ADDITIONAL_DATABASE_NAME = "foo";
    private static Set<String> defaultDatabases = Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME );
    private static Set<String> databasesWithAdditional = Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, ADDITIONAL_DATABASE_NAME );

    private static int additionalRRId = 127;
    private static int additionalCoreId = 128;

    private static int numCores = 3;
    private static int numRRs = 2;

    private static int timeout = 30;

    @Nested
    @TestDirectoryExtension
    @ClusterExtension
    class HappyPath
    {
        @Inject
        private ClusterFactory clusterFactory;
        @Inject
        private FileSystemAbstraction fs;
        private Cluster cluster;

        @BeforeAll
        void startCluster() throws Exception
        {
            var config = ClusterConfig.clusterConfig()
                    .withNumberOfCoreMembers( numCores )
                    .withNumberOfReadReplicas( numRRs );
            cluster = clusterFactory.createCluster( config );
            cluster.start();
            cluster.awaitLeader( DEFAULT_DATABASE_NAME );
        }

        @AfterEach
        void resetCluster() throws Exception
        {
            // check if additional cores/rrs exist first then remove them
            var additionalCore = cluster.getCoreMemberById( additionalCoreId );
            if ( additionalCore != null )
            {
                var homeDir = additionalCore.homeDir();
                cluster.removeCoreMember( additionalCore );
                fs.deleteRecursively( homeDir );
            }

            var additionalRR = cluster.getReadReplicaById( additionalRRId );
            if ( additionalRR != null )
            {
                var homeDir = additionalRR.homeDir();
                cluster.removeReadReplica( additionalRR );
                fs.deleteRecursively( homeDir );
            }

            // drop the additional database if it exists
            cluster.systemTx( ( db, tx ) ->
            {
                tx.execute( "DROP DATABASE " + ADDITIONAL_DATABASE_NAME + " IF EXISTS" );
                tx.commit();
            } );

            assertEventually( "SHOW DATABASE returns no members hosting additional database",
                    () -> membersHostingDatabase( ADDITIONAL_DATABASE_NAME, cluster ), is( empty() ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return one row per default database per initial cluster member", () -> showDatabases( cluster ),
                    hasSize( ( numCores + numRRs ) * defaultDatabases.size() ), timeout, SECONDS );
        }

        @Test
        void shouldShowNewCoreMembers() throws Exception
        {
            // given
            var initialClusterSize = cluster.allMembers().size();
            var initialShowDatabases = showDatabases( cluster );
            assertEquals( initialClusterSize * defaultDatabases.size(), initialShowDatabases.size(),
                    "SHOW DATABASES should return one row per database per cluster member" );
            assertThat( "SHOW DATABASES should return 2 followers per database", initialShowDatabases,
                    containsRole( "follower", defaultDatabases, 2 ) );
            assertThat( "SHOW DATABASES should return 1 leader per database", initialShowDatabases,
                    containsRole( "leader", defaultDatabases, 1 ) );
            assertThat( "SHOW DATABASES should return 2 replicas per database", initialShowDatabases,
                    containsRole( "read_replica", defaultDatabases, 2 ) );

            // when
            var newCore = cluster.addCoreMemberWithId( additionalCoreId );
            newCore.start();
            var newAddress = newCore.boltAdvertisedAddress();

            // then
            var newClusterSize = initialClusterSize + 1;
            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                    hasSize( newClusterSize * defaultDatabases.size() ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                    containsRole( "leader", defaultDatabases, 1 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 followers per database", () -> showDatabases( cluster ),
                    containsRole( "follower", defaultDatabases, 3 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas per database", () -> showDatabases( cluster ),
                    containsRole( "read_replica", defaultDatabases, 2 ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should return member with address %s for all databases", newAddress ),
                    () -> databasesHostedByMember( newAddress, cluster ), equalTo( defaultDatabases ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for member with address %s, for all databases", newAddress ),
                    () -> membersHaveStateForDatabases( Set.of( newAddress ), defaultDatabases, STARTED, cluster ), is( true ), timeout, SECONDS );
        }

        @Test
        void shouldShowNewReadReplicaMembers() throws Exception
        {
            // given
            var initialClusterSize = cluster.allMembers().size();
            var initialShowDatabases = showDatabases( cluster );
            assertEquals( initialClusterSize * defaultDatabases.size(), initialShowDatabases.size(),
                    "SHOW DATABASES should return one row per database per cluster member" );
            assertThat( "SHOW DATABASES should return 2 followers per database", initialShowDatabases,
                    containsRole( "follower", defaultDatabases, 2 ) );
            assertThat( "SHOW DATABASES should return 1 leader per database", initialShowDatabases,
                    containsRole( "leader", defaultDatabases, 1 ) );
            assertThat( "SHOW DATABASES should return 2 replicas per database", initialShowDatabases,
                    containsRole( "read_replica", defaultDatabases, 2 ) );

            // when

            var newReplica = cluster.addReadReplicaWithId( additionalRRId );
            newReplica.start();
            var newAddress = newReplica.boltAdvertisedAddress();

            // then
            var newClusterSize = initialClusterSize + 1;
            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                    hasSize( newClusterSize * defaultDatabases.size() ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                    containsRole( "leader", defaultDatabases, 1 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                    containsRole( "follower", defaultDatabases, 2 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 replicas per database", () -> showDatabases( cluster ),
                    containsRole( "read_replica", defaultDatabases, 3 ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should return member with address %s for all databases", newAddress ),
                    () -> databasesHostedByMember( newAddress, cluster ), equalTo( defaultDatabases ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for member with address %s, for all databases", newAddress ),
                    () -> membersHaveStateForDatabases( Set.of( newAddress ), defaultDatabases, STARTED, cluster ), is( true ), timeout, SECONDS );
        }

        @Test
        void shouldNotShowRemovedCoreMembers() throws Exception
        {
            // given
            var initialAddresses = cluster.allMembers().stream().map( ClusterMember::boltAdvertisedAddress ).collect( Collectors.toSet() );
            var newCore = cluster.addCoreMemberWithId( additionalCoreId );
            newCore.start();
            var newAddress = newCore.boltAdvertisedAddress();
            var clusterAddresses = new HashSet<>( initialAddresses );
            clusterAddresses.add( newAddress );

            var initialClusterSize = cluster.allMembers().size();
            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                    hasSize( initialClusterSize * defaultDatabases.size() ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                    containsRole( "leader", defaultDatabases, 1 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                    containsRole( "follower", defaultDatabases, 3 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 replicas per database", () -> showDatabases( cluster ),
                    containsRole( "read_replica", defaultDatabases, 2 ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for all databases", clusterAddresses ),
                    () -> membersHaveStateForDatabases( clusterAddresses, defaultDatabases, STARTED, cluster ), is( true ), timeout, SECONDS );

            // when
            cluster.removeCoreMemberWithServerId( additionalCoreId );

            // then
            var newClusterSize = initialClusterSize - 1;
            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                    hasSize( newClusterSize * defaultDatabases.size() ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                    containsRole( "leader", defaultDatabases, 1 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                    containsRole( "follower", defaultDatabases, 2 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas per database", () -> showDatabases( cluster ),
                    containsRole( "read_replica", defaultDatabases, 2 ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should return no rows for member %s", newAddress ),
                    () -> databasesHostedByMember( newAddress, cluster ), equalTo( emptySet() ), timeout, SECONDS );
        }

        @Test
        void shouldNotShowRemovedReadReplicaMembers() throws Exception
        {
            // given
            var initialAddresses = cluster.allMembers().stream().map( ClusterMember::boltAdvertisedAddress ).collect( Collectors.toSet() );
            var newReplica = cluster.addReadReplicaWithId( additionalRRId );
            newReplica.start();
            var newAddress = newReplica.boltAdvertisedAddress();
            var clusterAddresses = new HashSet<>( initialAddresses );
            clusterAddresses.add( newAddress );

            var initialClusterSize = cluster.allMembers().size();

            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                    hasSize( initialClusterSize * defaultDatabases.size() ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                    containsRole( "leader", defaultDatabases, 1 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                    containsRole( "follower", defaultDatabases, 2 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 replicas per database", () -> showDatabases( cluster ),
                    containsRole( "read_replica", defaultDatabases, 3 ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for all databases", clusterAddresses ),
                    () -> membersHaveStateForDatabases( clusterAddresses, defaultDatabases, STARTED, cluster ), is( true ), timeout, SECONDS );

            // when
            cluster.removeReadReplicaWithMemberId( additionalRRId );

            // then
            var newClusterSize = initialClusterSize - 1;
            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                    hasSize( newClusterSize * defaultDatabases.size() ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                    containsRole( "leader", defaultDatabases, 1 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                    containsRole( "follower", defaultDatabases, 2 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas per database", () -> showDatabases( cluster ),
                    containsRole( "read_replica", defaultDatabases, 2 ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should return no rows for member %s", newAddress ),
                    () -> databasesHostedByMember( newAddress, cluster ), equalTo( emptySet() ), timeout, SECONDS );
        }

        @Test
        void shouldDisplayDatabaseStatusChanges() throws Exception
        {
            // given
            createDatabase( ADDITIONAL_DATABASE_NAME, cluster );
            cluster.awaitLeader( ADDITIONAL_DATABASE_NAME );

            var clusterSize = cluster.allMembers().size();
            var clusterAddresses = cluster.allMembers().stream()
                    .map( ClusterMember::boltAdvertisedAddress )
                    .collect( Collectors.toSet() );

            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                    hasSize( clusterSize * databasesWithAdditional.size() ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                    containsRole( "leader", databasesWithAdditional, 1 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                    containsRole( "follower", databasesWithAdditional, 2 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas per database", () -> showDatabases( cluster ),
                    containsRole( "read_replica", databasesWithAdditional, 2 ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for all databases", clusterAddresses ),
                    () -> membersHaveStateForDatabases( clusterAddresses, databasesWithAdditional, STARTED, cluster ),
                    is( true ), timeout, SECONDS );

            // when
            stopDatabase( ADDITIONAL_DATABASE_NAME, cluster );

            // then
            assertEventually( format( "SHOW DATABASES should show Stopped status for members %s, for additional database", clusterAddresses ),
                    () -> membersHaveStateForDatabases( clusterAddresses, singleton( ADDITIONAL_DATABASE_NAME ), STOPPED, cluster ),
                    is( true ), timeout, SECONDS );
            assertEventually(  "SHOW DATABASES should show unknown role for all members for stopped additional database", () -> showDatabases( cluster ),
                    containsRole( "unknown", singleton( ADDITIONAL_DATABASE_NAME ), clusterSize ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should still show Started status for members %s, for default databases", clusterAddresses ),
                    () -> membersHaveStateForDatabases( clusterAddresses, defaultDatabases, STARTED, cluster ),
                    is( true ), timeout, SECONDS );
        }

        @Test
        void shouldShowAdditionalDatabasesOnAllMembers() throws Exception
        {
            // given
            var clusterSize = cluster.allMembers().size();
            var clusterAddresses = cluster.allMembers().stream()
                    .map( ClusterMember::boltAdvertisedAddress )
                    .collect( Collectors.toSet() );

            var initialShowDatabases = showDatabases( cluster );
            assertEquals( clusterSize * defaultDatabases.size(), initialShowDatabases.size(),
                    "SHOW DATABASES should return one row per database per cluster member" );
            assertThat( "SHOW DATABASES should return 2 followers per database", initialShowDatabases,
                    containsRole( "follower", defaultDatabases, 2 ) );
            assertThat( "SHOW DATABASES should return 1 leader per database", initialShowDatabases,
                    containsRole( "leader", defaultDatabases, 1 ) );
            assertThat( "SHOW DATABASES should return 2 replicas per database", initialShowDatabases,
                    containsRole( "read_replica", defaultDatabases, 2 ) );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for all databases", clusterAddresses ),
                    () -> membersHaveStateForDatabases( clusterAddresses, defaultDatabases, STARTED, cluster ), is( true ), timeout, SECONDS );

            // when
            createDatabase( ADDITIONAL_DATABASE_NAME, cluster );

            // then
            assertEventually( "SHOW DATABASES should return 1 leader for foo", () -> showDatabases( cluster ),
                    containsRole( "leader", singleton( ADDITIONAL_DATABASE_NAME ), 1 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers for foo", () -> showDatabases( cluster ),
                    containsRole( "follower", singleton( ADDITIONAL_DATABASE_NAME ), 2 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas for foo", () -> showDatabases( cluster ),
                    containsRole( "read_replica", singleton( ADDITIONAL_DATABASE_NAME ), 2 ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for foo", clusterAddresses ),
                    () -> membersHaveStateForDatabases( clusterAddresses, singleton( ADDITIONAL_DATABASE_NAME ), STARTED, cluster ),
                    is( true ), timeout, SECONDS );
        }

        @Test
        void shouldNotShowDroppedDatabasesOnAnyMembers() throws Exception
        {
            // given
            createDatabase( ADDITIONAL_DATABASE_NAME, cluster );
            cluster.awaitLeader( ADDITIONAL_DATABASE_NAME );

            var clusterSize = cluster.allMembers().size();
            var clusterAddresses = cluster.allMembers().stream()
                    .map( ClusterMember::boltAdvertisedAddress )
                    .collect( Collectors.toSet() );

            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                    hasSize( clusterSize * databasesWithAdditional.size() ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                    containsRole( "leader", databasesWithAdditional, 1 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                    containsRole( "follower", databasesWithAdditional, 2 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas per database", () -> showDatabases( cluster ),
                    containsRole( "read_replica", databasesWithAdditional, 2 ), timeout, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for all databases", clusterAddresses ),
                    () -> membersHaveStateForDatabases( clusterAddresses, databasesWithAdditional, STARTED, cluster ), is( true ), timeout, SECONDS );

            // when
            dropDatabase( ADDITIONAL_DATABASE_NAME, cluster );

            // then
            assertEventually( "SHOW DATABASES should return no rows for additional database",
                    () -> membersHostingDatabase( ADDITIONAL_DATABASE_NAME, cluster ), is( empty() ), timeout, SECONDS );
        }
    }

    @Nested
    @ClusterExtension
    @TestInstance( PER_METHOD )
    class SadPath
    {
        @Inject
        private ClusterFactory clusterFactory;
        private Cluster cluster;

        @BeforeEach
        void startCluster() throws Exception
        {
            var config = ClusterConfig.clusterConfig()
                    .withNumberOfCoreMembers( numCores )
                    .withNumberOfReadReplicas( numRRs );
            cluster = clusterFactory.createCluster( config );
            cluster.start();
            cluster.awaitLeader( DEFAULT_DATABASE_NAME );
        }

        @Test
        void shouldDisplayErrorForFailedDatabases() throws Exception
        {
            // given
            var initialClusterAddresses = cluster.allMembers().stream()
                    .map( ClusterMember::boltAdvertisedAddress )
                    .collect( Collectors.toSet() );

            // one follower which refuses to be leader configured with a max # databases of 2
            // one rr configured with a max # databases of 2
            var misConfiguredCore = cluster.addCoreMemberWithId( additionalCoreId );
            misConfiguredCore.updateConfig( EnterpriseEditionSettings.maxNumberOfDatabases, 2L );
            misConfiguredCore.updateConfig( CausalClusteringSettings.refuse_to_be_leader, true );
            var misConfiguredRR = cluster.addReadReplicaWithId( additionalRRId );
            misConfiguredRR.updateConfig( EnterpriseEditionSettings.maxNumberOfDatabases, 2L );

            misConfiguredCore.start();
            misConfiguredRR.start();

            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                    hasSize( cluster.allMembers().size() * defaultDatabases.size() ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                    containsRole( "leader", defaultDatabases, 1 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 followers per database", () -> showDatabases( cluster ),
                    containsRole( "follower", defaultDatabases, 3 ), timeout, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 read replicas per database", () -> showDatabases( cluster ),
                    containsRole( "read_replica", defaultDatabases, 3 ), timeout, SECONDS );

            // when
            createDatabase( ADDITIONAL_DATABASE_NAME, cluster );

            // then
            assertEventually( "SHOW DATABASES should return 2 rows with an error for database foo", () -> showDatabases( cluster ),
                    containsError( "The total limit of databases is already reached", "foo", 2 ), 60, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for database foo", initialClusterAddresses ),
                    () -> membersHaveStateForDatabases( initialClusterAddresses, singleton( ADDITIONAL_DATABASE_NAME ), STARTED, cluster ),
                    is( true ), timeout, SECONDS );
        }
    }

    private static Matcher<Collection<ShowDatabasesResultRow>> containsRole( String expectedRole, Set<String> databaseNames, long expectedCount )
    {
        Iterable<Matcher<? super Collection<ShowDatabasesResultRow>>> allMatchers = databaseNames.stream()
                .map( databaseName -> containsRole( expectedRole, databaseName, expectedCount ) )
                .collect( Collectors.toList() );
        return Matchers.allOf( allMatchers );
    }

    private static Matcher<? super Collection<ShowDatabasesResultRow>> containsRole( String expectedRole, String databaseName, long expectedCount )
    {
        var featureDescription = format( "Counts the number of members with role %s for the database %s", expectedRole, databaseName );
        return new FeatureMatcher<>( equalTo( expectedCount ), featureDescription, "count" )
        {
            @Override
            protected Long featureValueOf( Collection<ShowDatabasesResultRow> showDatabasesResultRows )
            {
                return showDatabasesResultRows.stream()
                        .filter( row -> Objects.equals( row.name(), databaseName ) &&
                                Objects.equals( row.role(), expectedRole ) )
                        .count();
            }
        };
    }

    private static Matcher<Collection<ShowDatabasesResultRow>> containsError( String expectedError, String databaseName, long expectedCount )
    {
        var featureDescription = format( "Counts the number of members with error %s for the database %s", expectedError, databaseName );
        return new FeatureMatcher<>( equalTo( expectedCount ), featureDescription, "count" )
        {
            @Override
            protected Long featureValueOf( Collection<ShowDatabasesResultRow> showDatabasesResultRows )
            {
                return showDatabasesResultRows.stream()
                        .filter( row -> Objects.equals( row.name(), databaseName ) && row.error().contains( expectedError ) )
                        .count();
            }
        };
    }

    private static Set<String> membersHostingDatabase( String databaseName, Cluster cluster ) throws Exception
    {
        return showDatabases( cluster ).stream()
                .filter( row -> Objects.equals( databaseName, row.name() ) )
                .map( ShowDatabasesHelpers.ShowDatabasesResultRow::address )
                .collect( Collectors.toSet() );
    }

    private static Set<String> databasesHostedByMember( String memberBoltAddress, Cluster cluster ) throws Exception
    {
        return showDatabases( cluster ).stream()
                .filter( row -> Objects.equals( memberBoltAddress, row.address() ) )
                .map( ShowDatabasesHelpers.ShowDatabasesResultRow::name )
                .collect( Collectors.toSet() );
    }

    private static boolean membersHaveStateForDatabases( Set<String> memberBoltAddresses, Set<String> databaseNames,
            OperatorState expectedState, Cluster cluster ) throws Exception
    {
        var statesByAddressAndName = showDatabases( cluster ).stream()
                .collect( Collectors.toMap( ClusteredShowDatabasesIT::boltDbNameCompositeKey, ShowDatabasesHelpers.ShowDatabasesResultRow::currentStatus ) );

        var expectedAddressDatabaseCombinations = memberBoltAddresses.stream()
                .flatMap( bolt -> databaseNames.stream().map( name -> Pair.of( bolt, name ) ) )
                .collect( Collectors.toSet() );

        var databasesExistOnExpectedMembers = statesByAddressAndName.keySet().containsAll( expectedAddressDatabaseCombinations );

        if ( !databasesExistOnExpectedMembers )
        {
            return false;
        }

        return expectedAddressDatabaseCombinations.stream()
                .allMatch( addressNameKey ->
                {
                    var actualState = statesByAddressAndName.get( addressNameKey );
                    return Objects.equals( expectedState.description(), actualState );
                } );
    }

    private static Pair<String,String> boltDbNameCompositeKey( ShowDatabasesResultRow row )
    {
        return Pair.of( row.address(), row.name() );
    }
}
