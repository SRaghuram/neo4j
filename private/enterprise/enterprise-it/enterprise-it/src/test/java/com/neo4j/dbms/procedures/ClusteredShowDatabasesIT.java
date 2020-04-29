/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.dbms.EnterpriseOperatorState;
import com.neo4j.dbms.ShowDatabasesHelpers;
import com.neo4j.dbms.ShowDatabasesHelpers.ShowDatabasesResultRow;
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.OperatorState;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.dropDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.showDatabases;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;
import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.allOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;
import static org.neo4j.test.conditions.Conditions.sizeCondition;

class ClusteredShowDatabasesIT
{

    private static final int LOCAL_STATE_CHANGE_TIMEOUT_SECONDS = 120;
    private static final String ADDITIONAL_DATABASE_NAME = "foo";
    private final Set<String> defaultDatabases = Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME );
    private final Set<String> databasesWithAdditional = Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, ADDITIONAL_DATABASE_NAME );

    private final int additionalRRId = 127;
    private final int additionalCoreId = 128;

    private final int numCores = 3;
    private final int numRRs = 2;

    private final int timeoutSeconds = 60;

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
            NamedDatabaseId namedDatabaseId;
            try
            {
                namedDatabaseId = getNamedDatabaseId( cluster, ADDITIONAL_DATABASE_NAME );
            }
            catch ( DatabaseNotFoundException e )
            {
                // database does not exist
                return;
            }
            cluster.systemTx( ( db, tx ) ->
                              {
                                  tx.execute( "DROP DATABASE " + ADDITIONAL_DATABASE_NAME + " IF EXISTS" );
                                  tx.commit();
                              } );

            waitForClusterToReachLocalState( cluster, namedDatabaseId, EnterpriseOperatorState.DROPPED );

            assertEventually( "SHOW DATABASE returns no members hosting additional database",
                              () -> membersHostingDatabase( ADDITIONAL_DATABASE_NAME, cluster ), Set::isEmpty, timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return one row per default database per initial cluster member", () -> showDatabases( cluster ),
                              result -> result.size() == ( numCores + numRRs ) * defaultDatabases.size(), timeoutSeconds, SECONDS );
        }

        @Test
        void shouldShowNewCoreMembers() throws Exception
        {
            // given
            var initialClusterSize = cluster.allMembers().size();
            var initialShowDatabases = showDatabases( cluster );
            assertEquals( initialClusterSize * defaultDatabases.size(), initialShowDatabases.size(),
                    "SHOW DATABASES should return one row per database per cluster member" );
            assertThat( initialShowDatabases ).as( "SHOW DATABASES should return 2 followers per database" )
                    .satisfies( containsRole( "follower", defaultDatabases, 2 ) );
            assertThat( initialShowDatabases ).as( "SHOW DATABASES should return 1 leader per database" )
                    .satisfies( containsRole( "leader", defaultDatabases, 1 ) );
            assertThat( initialShowDatabases ).as( "SHOW DATABASES should return 2 replicas per database" )
                    .satisfies( containsRole( "read_replica", defaultDatabases, 2 ) );

            // when
            var newCore = cluster.addCoreMemberWithId( additionalCoreId );
            newCore.start();
            var newAddress = newCore.boltAdvertisedAddress();

            // then
            var newClusterSize = initialClusterSize + 1;
            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                              sizeCondition( newClusterSize * defaultDatabases.size() ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                              containsRole( "leader", defaultDatabases, 1 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 followers per database", () -> showDatabases( cluster ),
                              containsRole( "follower", defaultDatabases, 3 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas per database", () -> showDatabases( cluster ),
                              containsRole( "read_replica", defaultDatabases, 2 ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should return member with address %s for all databases", newAddress ),
                              () -> databasesHostedByMember( newAddress, cluster ), equalityCondition( defaultDatabases ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for member with address %s, for all databases", newAddress ),
                              () -> showDatabases( cluster ), membersHaveStateForDatabases( Set.of( newAddress ), defaultDatabases, STARTED ), timeoutSeconds,
                              SECONDS );
        }

        @Test
        void shouldShowNewReadReplicaMembers() throws Exception
        {
            // given
            var initialClusterSize = cluster.allMembers().size();
            var initialShowDatabases = showDatabases( cluster );
            assertEquals( initialClusterSize * defaultDatabases.size(), initialShowDatabases.size(),
                    "SHOW DATABASES should return one row per database per cluster member" );
            assertThat( initialShowDatabases ).as( "SHOW DATABASES should return 2 followers per database" )
                    .satisfies( containsRole( "follower", defaultDatabases, 2 ) );
            assertThat( initialShowDatabases ).as( "SHOW DATABASES should return 1 leader per database" )
                    .satisfies( containsRole( "leader", defaultDatabases, 1 ) );
            assertThat( initialShowDatabases ).as( "SHOW DATABASES should return 2 replicas per database" )
                    .satisfies( containsRole( "read_replica", defaultDatabases, 2 ) );

            // when

            var newReplica = cluster.addReadReplicaWithId( additionalRRId );
            newReplica.start();
            var newAddress = newReplica.boltAdvertisedAddress();

            // then
            var newClusterSize = initialClusterSize + 1;
            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                              sizeCondition( newClusterSize * defaultDatabases.size() ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                              containsRole( "leader", defaultDatabases, 1 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                              containsRole( "follower", defaultDatabases, 2 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 replicas per database", () -> showDatabases( cluster ),
                              containsRole( "read_replica", defaultDatabases, 3 ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should return member with address %s for all databases", newAddress ),
                              () -> databasesHostedByMember( newAddress, cluster ), equalityCondition( defaultDatabases ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for member with address %s, for all databases", newAddress ),
                              () -> showDatabases( cluster ), membersHaveStateForDatabases( Set.of( newAddress ), defaultDatabases, STARTED ), timeoutSeconds,
                              SECONDS );
        }

        @Test
        void shouldNotShowRemovedCoreMembers()
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
                              sizeCondition( initialClusterSize * defaultDatabases.size() ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                              containsRole( "leader", defaultDatabases, 1 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                              containsRole( "follower", defaultDatabases, 3 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 replicas per database", () -> showDatabases( cluster ),
                              containsRole( "read_replica", defaultDatabases, 2 ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for all databases", clusterAddresses ),
                              () -> showDatabases( cluster ), membersHaveStateForDatabases( clusterAddresses, defaultDatabases, STARTED ), timeoutSeconds,
                              SECONDS );

            // when
            cluster.removeCoreMemberWithServerId( additionalCoreId );

            // then
            var newClusterSize = initialClusterSize - 1;
            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                              sizeCondition( newClusterSize * defaultDatabases.size() ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                              containsRole( "leader", defaultDatabases, 1 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                              containsRole( "follower", defaultDatabases, 2 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas per database", () -> showDatabases( cluster ),
                              containsRole( "read_replica", defaultDatabases, 2 ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should return no rows for member %s", newAddress ),
                              () -> databasesHostedByMember( newAddress, cluster ), equalityCondition( emptySet() ), timeoutSeconds, SECONDS );
        }

        @Test
        void shouldNotShowRemovedReadReplicaMembers()
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
                              sizeCondition( initialClusterSize * defaultDatabases.size() ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                              containsRole( "leader", defaultDatabases, 1 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                              containsRole( "follower", defaultDatabases, 2 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 replicas per database", () -> showDatabases( cluster ),
                              containsRole( "read_replica", defaultDatabases, 3 ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for all databases", clusterAddresses ),
                              () -> showDatabases( cluster ), membersHaveStateForDatabases( clusterAddresses, defaultDatabases, STARTED ), timeoutSeconds,
                              SECONDS );

            // when
            cluster.removeReadReplicaWithMemberId( additionalRRId );

            // then
            var newClusterSize = initialClusterSize - 1;
            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                              sizeCondition( newClusterSize * defaultDatabases.size() ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                              containsRole( "leader", defaultDatabases, 1 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                              containsRole( "follower", defaultDatabases, 2 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas per database", () -> showDatabases( cluster ),
                              containsRole( "read_replica", defaultDatabases, 2 ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should return no rows for member %s", newAddress ),
                              () -> databasesHostedByMember( newAddress, cluster ), equalityCondition( emptySet() ), timeoutSeconds, SECONDS );
        }

        @Test
        void shouldDisplayDatabaseStatusChanges() throws Exception
        {
            // given
            createDatabase( ADDITIONAL_DATABASE_NAME, cluster );
            var additionalDatabaseId = getNamedDatabaseId( cluster, ADDITIONAL_DATABASE_NAME );
            waitForClusterToReachLocalState( cluster, additionalDatabaseId, STARTED );
            cluster.awaitLeader( ADDITIONAL_DATABASE_NAME );

            var clusterSize = cluster.allMembers().size();
            var clusterAddresses = cluster.allMembers().stream()
                    .map( ClusterMember::boltAdvertisedAddress )
                    .collect( Collectors.toSet() );

            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                              sizeCondition( clusterSize * databasesWithAdditional.size() ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                              containsRole( "leader", databasesWithAdditional, 1 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                              containsRole( "follower", databasesWithAdditional, 2 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas per database", () -> showDatabases( cluster ),
                              containsRole( "read_replica", databasesWithAdditional, 2 ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for all databases", clusterAddresses ),
                              () -> showDatabases( cluster ), membersHaveStateForDatabases( clusterAddresses, databasesWithAdditional, STARTED ),
                              timeoutSeconds, SECONDS );

            // when
            stopDatabase( ADDITIONAL_DATABASE_NAME, cluster );
            waitForClusterToReachLocalState( cluster, additionalDatabaseId, STOPPED );

            // then
            assertEventually( format( "SHOW DATABASES should show Stopped status for members %s, for additional database", clusterAddresses ),
                              () -> showDatabases( cluster ), membersHaveStateForDatabases( clusterAddresses, singleton( ADDITIONAL_DATABASE_NAME ), STOPPED ),
                              timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should show unknown role for all members for stopped additional database", () -> showDatabases( cluster ),
                              containsRole( "unknown", singleton( ADDITIONAL_DATABASE_NAME ), clusterSize ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should still show Started status for members %s, for default databases", clusterAddresses ),
                              () -> showDatabases( cluster ), membersHaveStateForDatabases( clusterAddresses, defaultDatabases, STARTED ), timeoutSeconds,
                              SECONDS );
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
            assertThat( initialShowDatabases ).as( "SHOW DATABASES should return 2 followers per database" )
                        .satisfies( containsRole( "follower", defaultDatabases, 2 ) );
            assertThat( initialShowDatabases ).as( "SHOW DATABASES should return 1 leader per database" )
                        .satisfies( containsRole( "leader", defaultDatabases, 1 ) );
            assertThat( initialShowDatabases ).as( "SHOW DATABASES should return 2 replicas per database" )
                        .satisfies( containsRole( "read_replica", defaultDatabases, 2 ) );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for all databases", clusterAddresses ),
                              () -> showDatabases( cluster ), membersHaveStateForDatabases( clusterAddresses, defaultDatabases, STARTED ), timeoutSeconds,
                              SECONDS );

            // when
            createDatabase( ADDITIONAL_DATABASE_NAME, cluster );
            waitForClusterToReachLocalState( cluster, getNamedDatabaseId( cluster, ADDITIONAL_DATABASE_NAME ), STARTED );

            // then
            assertEventually( "SHOW DATABASES should return 1 leader for foo", () -> showDatabases( cluster ),
                              containsRole( "leader", singleton( ADDITIONAL_DATABASE_NAME ), 1 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers for foo", () -> showDatabases( cluster ),
                              containsRole( "follower", singleton( ADDITIONAL_DATABASE_NAME ), 2 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas for foo", () -> showDatabases( cluster ),
                              containsRole( "read_replica", singleton( ADDITIONAL_DATABASE_NAME ), 2 ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for foo", clusterAddresses ),
                              () -> showDatabases( cluster ), membersHaveStateForDatabases( clusterAddresses, singleton( ADDITIONAL_DATABASE_NAME ), STARTED ),
                              timeoutSeconds, SECONDS );
        }

        @Test
        void shouldNotShowDroppedDatabasesOnAnyMembers() throws Exception
        {
            // given
            createDatabase( ADDITIONAL_DATABASE_NAME, cluster );
            var additionalDatabaseId = getNamedDatabaseId( cluster, ADDITIONAL_DATABASE_NAME );
            waitForClusterToReachLocalState( cluster, additionalDatabaseId, STARTED );
            cluster.awaitLeader( ADDITIONAL_DATABASE_NAME );

            var clusterSize = cluster.allMembers().size();
            var clusterAddresses = cluster.allMembers().stream()
                    .map( ClusterMember::boltAdvertisedAddress )
                    .collect( Collectors.toSet() );

            assertEventually( "SHOW DATABASES should return one row per database per cluster member", () -> showDatabases( cluster ),
                              sizeCondition( clusterSize * databasesWithAdditional.size() ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                              containsRole( "leader", databasesWithAdditional, 1 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 followers per database", () -> showDatabases( cluster ),
                              containsRole( "follower", databasesWithAdditional, 2 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 2 replicas per database", () -> showDatabases( cluster ),
                              containsRole( "read_replica", databasesWithAdditional, 2 ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for all databases", clusterAddresses ),
                              () -> showDatabases( cluster ), membersHaveStateForDatabases( clusterAddresses, databasesWithAdditional, STARTED ),
                              timeoutSeconds, SECONDS );

            // when
            dropDatabase( ADDITIONAL_DATABASE_NAME, cluster, false );
            waitForClusterToReachLocalState( cluster, additionalDatabaseId, DROPPED );

            // then
            assertEventually( "SHOW DATABASES should return no rows for additional database",
                              () -> membersHostingDatabase( ADDITIONAL_DATABASE_NAME, cluster ), Set::isEmpty, timeoutSeconds, SECONDS );
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
            var initialMembers = cluster.allMembers();
            var initialClusterAddresses = initialMembers.stream()
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
                              sizeCondition( cluster.allMembers().size() * defaultDatabases.size() ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 1 leader per database", () -> showDatabases( cluster ),
                              containsRole( "leader", defaultDatabases, 1 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 followers per database", () -> showDatabases( cluster ),
                              containsRole( "follower", defaultDatabases, 3 ), timeoutSeconds, SECONDS );
            assertEventually( "SHOW DATABASES should return 3 read replicas per database", () -> showDatabases( cluster ),
                              containsRole( "read_replica", defaultDatabases, 3 ), timeoutSeconds, SECONDS );

            // when
            createDatabase( ADDITIONAL_DATABASE_NAME, cluster );
            waitForClusterToReachLocalState( initialMembers, getNamedDatabaseId( cluster, ADDITIONAL_DATABASE_NAME ), STARTED );

            // then
            assertEventually( "SHOW DATABASES should return 2 rows with an error for database foo", () -> showDatabases( cluster ),
                              containsError( "The total limit of databases is already reached", "foo", 2 ), timeoutSeconds, SECONDS );
            assertEventually( format( "SHOW DATABASES should show Started status for members %s, for database foo", initialClusterAddresses ),
                              () -> showDatabases( cluster ),
                              membersHaveStateForDatabases( initialClusterAddresses, singleton( ADDITIONAL_DATABASE_NAME ), STARTED ), timeoutSeconds,
                              SECONDS );
        }
    }

    private static Condition<List<? extends ShowDatabasesResultRow>> containsRole( String expectedRole, Set<String> databaseNames, long expectedCount )
    {
        var allConditions = databaseNames.stream()
                .map( databaseName -> containsRole( expectedRole, databaseName, expectedCount ) ).collect( toList() );
        return Assertions.allOf( allConditions );
    }

    private static Condition<Collection<? extends ShowDatabasesResultRow>> containsRole( String expectedRole, String databaseName, long expectedCount )
    {
        var featureDescription = format( "Expected exactly %s members with role %s for the database %s", expectedCount, expectedRole, databaseName );
        return new Condition<>( showDatabasesResultRows -> showDatabasesResultRows.stream()
                                                                   .filter( row -> Objects.equals( row.name(), databaseName ) &&
                                                                                   Objects.equals( row.role(), expectedRole ) )
                                                                   .count() == expectedCount, featureDescription );
    }

    private static Condition<Collection<ShowDatabasesResultRow>> containsError( String expectedError, String databaseName, long expectedCount )
    {
        var description = format( "Counts the number of members with error %s for the database %s", expectedError, databaseName );
        return new Condition<>( result -> result.stream()
                .filter( row -> Objects.equals( row.name(), databaseName ) && row.error().contains( expectedError ) ).count() == expectedCount, description );
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

    private static Condition<List<ShowDatabasesResultRow>> membersHaveStateForDatabases( Set<String> memberBoltAddresses, Set<String> databaseNames,
            EnterpriseOperatorState expectedState )
    {
        var expectedAddressDatabaseCombinations = memberBoltAddresses.stream()
                .flatMap( bolt -> databaseNames.stream().map( name -> Pair.of( bolt, name ) ) )
                .collect( Collectors.toSet() );
        return allOf( containsAllAddressesCondition( databaseNames, expectedAddressDatabaseCombinations ),
                      expectedStateForMembersCondition( memberBoltAddresses, databaseNames, expectedState, expectedAddressDatabaseCombinations ) );
    }

    private static Condition<List<ShowDatabasesResultRow>> expectedStateForMembersCondition( Set<String> memberBoltAddresses,
            Set<String> databaseNames, EnterpriseOperatorState expectedState, Set<Pair<String,String>> expectedAddressDatabaseCombinations )
    {
        return new Condition<>( resultRows -> expectedAddressDatabaseCombinations.stream().allMatch( addressNameKey ->
                          {
                              var statesByAddressAndName = resultRows.stream().collect(
                                              toMap( ClusteredShowDatabasesIT::boltDbNameCompositeKey,
                                                     ShowDatabasesResultRow::currentStatus ) );
                              var actualState = statesByAddressAndName.get( addressNameKey );
                              return Objects.equals( expectedState.description(), actualState );
                          } ),
                                "Expected SHOW DATABASE result for members %s for databases %s to all be in state %s", memberBoltAddresses,
                                databaseNames, expectedState );
    }

    private static Condition<List<ShowDatabasesResultRow>> containsAllAddressesCondition( Set<String> databaseNames,
            Set<Pair<String,String>> expectedAddressDatabaseCombinations )
    {
        return new Condition<>(
                              resultRows -> resultRows.stream().map( ClusteredShowDatabasesIT::boltDbNameCompositeKey ).collect( toSet() )
                                      .containsAll( expectedAddressDatabaseCombinations ),
                              "Expected SHOW DATABASE result for databases %s to contain all following bolt addresses: %s", databaseNames,
                              expectedAddressDatabaseCombinations );
    }

    private static void waitForClusterToReachLocalState( Cluster cluster, NamedDatabaseId namedDatabaseId, EnterpriseOperatorState operatorState )
    {
        waitForClusterToReachLocalState( cluster.allMembers(), namedDatabaseId, operatorState );
    }

    private static void waitForClusterToReachLocalState( Set<ClusterMember> members, NamedDatabaseId namedDatabaseId, EnterpriseOperatorState operatorState )
    {
        var databaseStateServices = databaseStateServices( members );
        assertEventually( () -> getOperatorStates( namedDatabaseId, databaseStateServices ), allStatesMatchCondition( operatorState ),
                          LOCAL_STATE_CHANGE_TIMEOUT_SECONDS, SECONDS );
    }

    private static NamedDatabaseId getNamedDatabaseId( Cluster cluster, String databaseName )
    {
        for ( CoreClusterMember coreMember : cluster.coreMembers() )
        {
            try
            {
                return coreMember.databaseId( databaseName );
            }
            catch ( DatabaseNotFoundException ignore )
            {
            }
        }
        throw new DatabaseNotFoundException( "Could not get database id for `" + databaseName + "` from any core member." );
    }

    private static List<OperatorState> getOperatorStates( NamedDatabaseId namedDatabaseId,
            Collection<DatabaseStateService> databaseStateServices )
    {
        return databaseStateServices.stream().map( databaseStateService ->
                                                           databaseStateService == null ? UNKNOWN : databaseStateService
                                                                   .stateOfDatabase( namedDatabaseId ) ).collect( toList() );
    }

    private static Condition<Collection<OperatorState>> allStatesMatchCondition( EnterpriseOperatorState enterpriseOperatorState )
    {
        return new Condition<>( states -> states.stream()
                .allMatch( operatorState -> operatorState == enterpriseOperatorState ), "Assume all databases has state " + enterpriseOperatorState );
    }

    private static Collection<DatabaseStateService> databaseStateServices( Set<ClusterMember> members )
    {
        return members.stream().map( member -> member.resolveDependency( SYSTEM_DATABASE_NAME, DatabaseStateService.class ) ).collect( toList() );
    }

    private static Pair<String,String> boltDbNameCompositeKey( ShowDatabasesResultRow row )
    {
        return Pair.of( row.address(), row.name() );
    }
}
