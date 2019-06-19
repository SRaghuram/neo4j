/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.database.ClusterDatabaseManagementIT.DatabaseAvailability.ABSENT;
import static com.neo4j.dbms.database.ClusterDatabaseManagementIT.DatabaseAvailability.AVAILABLE;
import static com.neo4j.dbms.database.ClusterDatabaseManagementIT.DatabaseAvailability.STOPPED;
import static com.neo4j.dbms.database.ClusterDatabaseManagementIT.DatabaseAvailability.UNDEFINED;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.test.assertion.Assert.assertEventually;

// TODO: Add tests for DROP DATABASE.
@ClusterExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
class ClusterDatabaseManagementIT
{
    private final int DEFAULT_TIMEOUT = 60;

    @Inject
    private ClusterFactory clusterFactory;

    private ClusterConfig clusterConfig = clusterConfig()
            .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" )
            .withSharedCoreParam( SecuritySettings.authentication_providers, SecuritySettings.NATIVE_REALM_NAME )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 );

    @Test
    void shouldReplicateDatabaseManagementOperations() throws Exception
    {
        // given
        var cluster = startCluster();
        assertTrue( allMembersHaveDatabaseState( ABSENT, cluster, "foo" ) );

        // when
        createDatabase( cluster, "foo" );
        createDatabase( cluster, "bar" );

        // then
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "bar" ), is( true ), DEFAULT_TIMEOUT, SECONDS );

        for ( int i = 0; i < 3; i++ )
        {
            assertCanStopStartDatabase( cluster, "foo" );
            assertCanStopStartDatabase( cluster, "bar" );
        }
    }

    private void assertCanStopStartDatabase( Cluster cluster, String databaseName ) throws Exception
    {
        // when
        stopDatabase( cluster, databaseName );

        // then
        assertEventually( () -> allMembersHaveDatabaseState( STOPPED, cluster, databaseName ), is( true ), DEFAULT_TIMEOUT, SECONDS );

        // when
        startDatabase( cluster, databaseName );

        // then
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, databaseName ), is( true ), DEFAULT_TIMEOUT, SECONDS );
    }

    @Test
    void shouldCreateDatabaseOnRejoiningMembers() throws Exception
    {
        // given
        var modifiedConfig = clusterConfig
                .withNumberOfCoreMembers( 4 )
                .withSharedCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3" );
        var cluster = startCluster( modifiedConfig );
        assertTrue( allMembersHaveDatabaseState( ABSENT, cluster, "foo" ) );

        var rejoiningMembers = oneCoreAndOneReadReplica( cluster );
        var remainingMembers = cluster.allMembers().filter( m -> !rejoiningMembers.contains( m ) ).collect( Collectors.toList() );

        rejoiningMembers.forEach( ClusterMember::shutdown );

        createDatabase( cluster, "foo" );
        assertEventually( () -> membersHaveDatabaseState( AVAILABLE, remainingMembers, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );

        // when
        rejoiningMembers.forEach( ClusterMember::start );

        // then
        assertEventually( () -> membersHaveDatabaseState( AVAILABLE, rejoiningMembers, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
    }

    @Test
    void shouldKeepStartedStateBetweenClusterRestarts() throws Exception
    {
        // given
        var cluster = startCluster();

        createDatabase( cluster, "foo" );
        createDatabase( cluster, "bar" );

        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "bar" ), is( true ), DEFAULT_TIMEOUT, SECONDS );

        // when
        restartCluster( cluster );

        // then
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "bar" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
    }

    @Test
    void shouldKeepStoppedStateBetweenClusterRestarts() throws Exception
    {
        // given
        var cluster = startCluster();

        createDatabase( cluster, "foo" );
        createDatabase( cluster, "bar" );

        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "bar" ), is( true ), DEFAULT_TIMEOUT, SECONDS );

        stopDatabase( cluster, "foo" );
        stopDatabase( cluster, "bar" );

        assertEventually( () -> allMembersHaveDatabaseState( STOPPED, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
        assertEventually( () -> allMembersHaveDatabaseState( STOPPED, cluster, "bar" ), is( true ), DEFAULT_TIMEOUT, SECONDS );

        // when
        restartCluster( cluster );

        // then
        assertEventually( () -> allMembersHaveDatabaseState( STOPPED, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
        assertEventually( () -> allMembersHaveDatabaseState( STOPPED, cluster, "bar" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
    }

    @Test
    void shouldKeepStartedStateBetweenMemberRestarts() throws Exception
    {
        // given
        var cluster = startCluster();
        createDatabase( cluster, "foo" );
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );

        var someMembers = oneCoreAndOneReadReplica( cluster );

        // when
        restartMembers( someMembers );

        // then
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
    }

    @Test
    void shouldKeepStoppedStateBetweenMemberRestarts() throws Exception
    {
        // given
        var cluster = startCluster();
        createDatabase( cluster, "foo" );
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
        stopDatabase( cluster, "foo" );
        assertEventually( () -> allMembersHaveDatabaseState( STOPPED, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );

        var someMembers = oneCoreAndOneReadReplica( cluster );

        // when
        restartMembers( someMembers );

        // then
        assertEventually( () -> allMembersHaveDatabaseState( STOPPED, cluster, "foo" ), is( true ), DEFAULT_TIMEOUT, SECONDS );
    }

    private void createDatabase( Cluster cluster, String databaseName ) throws Exception
    {
        cluster.coreTx( SYSTEM_DATABASE_NAME, ( sys, tx ) ->
        {
            sys.execute( "CREATE DATABASE " + databaseName );
            tx.success();
        } );
    }

    private void startDatabase( Cluster cluster, String databaseName ) throws Exception
    {
        cluster.coreTx( SYSTEM_DATABASE_NAME, ( sys, tx ) ->
        {
            sys.execute( "START DATABASE " + databaseName );
            tx.success();
        } );
    }

    private void stopDatabase( Cluster cluster, String databaseName ) throws Exception
    {
        cluster.coreTx( SYSTEM_DATABASE_NAME, ( sys, tx ) ->
        {
            sys.execute( "STOP DATABASE " + databaseName );
            tx.success();
        } );
    }

    private void restartMembers( Set<ClusterMember> restartingMembers )
    {
        restartingMembers.forEach( ClusterMember::shutdown );
        restartingMembers.forEach( ClusterMember::start );
    }

    private Cluster startCluster() throws InterruptedException, ExecutionException
    {
        return startCluster( this.clusterConfig );
    }

    private Cluster startCluster( ClusterConfig clusterConfig ) throws InterruptedException, ExecutionException
    {
        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        assertDefaultDatabasesAreAvailable( cluster );
        return cluster;
    }

    private void restartCluster( Cluster cluster ) throws InterruptedException, ExecutionException
    {
        cluster.shutdown();
        cluster.start();

        assertDefaultDatabasesAreAvailable( cluster );
    }

    private void assertDefaultDatabasesAreAvailable( Cluster cluster ) throws InterruptedException
    {
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, SYSTEM_DATABASE_NAME ), is( true ), DEFAULT_TIMEOUT, SECONDS );
        assertEventually( () -> allMembersHaveDatabaseState( AVAILABLE, cluster, DEFAULT_DATABASE_NAME ), is( true ), DEFAULT_TIMEOUT, SECONDS );
    }

    private Set<ClusterMember> oneCoreAndOneReadReplica( Cluster cluster )
    {
        return asSet( cluster.getCoreMemberById( 0 ), cluster.getReadReplicaById( 0 ) );
    }

    private static boolean allMembersHaveDatabaseState( DatabaseAvailability expected, Cluster cluster, String databaseName )
    {
        return membersHaveDatabaseState( expected, cluster.allMembers().collect( Collectors.toList() ), databaseName );
    }

    private static boolean membersHaveDatabaseState( DatabaseAvailability expected, Collection<ClusterMember> members, String databaseName )
    {
        for ( ClusterMember member : members )
        {
            DatabaseAvailability availability = memberDatabaseState( member, databaseName );
            if ( availability != expected )
            {
                return false;
            }
        }
        return true;
    }

    private static DatabaseAvailability memberDatabaseState( ClusterMember member, String databaseName )
    {
        GraphDatabaseService db;

        try
        {
            db = member.managementService().database( databaseName );
        }
        catch ( DatabaseNotFoundException ignored )
        {
            return ABSENT;
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.success();
        }
        catch ( DatabaseShutdownException ignored )
        {
            return STOPPED;
        }
        catch ( TransactionFailureException ignored )
        {
            // This should be transient!
            return UNDEFINED;
        }

        return AVAILABLE;
    }

    enum DatabaseAvailability
    {
        /**
         * The state is undefined (not to be confused with unavailable). Do not assert on this value in tests!
         */
        UNDEFINED,
        /**
         * A database with the specified name does not exist.
         */
        ABSENT,
        /**
         * The database is stopped.
         */
        STOPPED,
        /**
         * The database is available for transactions.
         */
        AVAILABLE,
    }
}
