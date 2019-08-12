/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.kernel.enterprise.api.security.CommercialSecurityContext;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.assertion.Assert;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseDoesNotExist;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.startDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

// TODO: Add tests for DROP DATABASE.
@ClusterExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
class ClusterDatabaseManagementIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private final ClusterConfig clusterConfig = clusterConfig()
            .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" )
            .withSharedCoreParam( SecuritySettings.authentication_providers, SecuritySettings.NATIVE_REALM_NAME )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 );

    @Test
    void shouldReplicateDatabaseManagementOperations() throws Exception
    {
        // given
        var cluster = startCluster();
        assertDatabaseDoesNotExist( "foo", cluster );

        // when
        createDatabase( "foo", cluster );
        createDatabase( "bar", cluster );

        // then
        assertDatabaseEventuallyStarted( "foo", cluster );
        assertDatabaseEventuallyStarted( "bar", cluster );

        for ( int i = 0; i < 3; i++ )
        {
            assertCanStopStartDatabase( "foo", cluster );
            assertCanStopStartDatabase( "bar", cluster );
        }
    }

    @Test
    void shouldCreateDatabaseOnRejoiningMembers() throws Exception
    {
        // given
        var modifiedConfig = clusterConfig
                .withNumberOfCoreMembers( 4 )
                .withSharedCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3" );
        var cluster = startCluster( modifiedConfig );
        assertDatabaseDoesNotExist( "foo", cluster );

        var rejoiningMembers = oneCoreAndOneReadReplica( cluster );
        var remainingMembers = cluster.allMembers().stream().filter( m -> !rejoiningMembers.contains( m ) ).collect( toSet() );

        rejoiningMembers.forEach( ClusterMember::shutdown );

        createDatabase( "foo", cluster );
        assertDatabaseEventuallyStarted( "foo", remainingMembers );

        // when
        rejoiningMembers.forEach( ClusterMember::start );

        // then
        assertDatabaseEventuallyStarted( "foo", rejoiningMembers );
    }

    @Test
    void shouldKeepStartedStateBetweenClusterRestarts() throws Exception
    {
        // given
        var cluster = startCluster();

        createDatabase( "foo", cluster );
        createDatabase( "bar", cluster );

        assertDatabaseEventuallyStarted( "foo", cluster );
        assertDatabaseEventuallyStarted( "bar", cluster );

        // when
        restartCluster( cluster );

        // then
        assertDatabaseEventuallyStarted( "foo", cluster );
        assertDatabaseEventuallyStarted( "bar", cluster );
    }

    @Test
    void shouldKeepStoppedStateBetweenClusterRestarts() throws Exception
    {
        // given
        var cluster = startCluster();

        createDatabase( "foo", cluster );
        createDatabase( "bar", cluster );

        assertDatabaseEventuallyStarted( "foo", cluster );
        assertDatabaseEventuallyStarted( "bar", cluster );

        stopDatabase( "foo", cluster );
        stopDatabase( "bar", cluster );

        assertDatabaseEventuallyStopped( "foo", cluster );
        assertDatabaseEventuallyStopped( "bar", cluster );

        // when
        restartCluster( cluster );

        // then
        assertDatabaseEventuallyStopped( "foo", cluster );
        assertDatabaseEventuallyStopped( "bar", cluster );
    }

    @Test
    void shouldKeepStartedStateBetweenMemberRestarts() throws Exception
    {
        // given
        var cluster = startCluster();
        createDatabase( "foo", cluster );
        assertDatabaseEventuallyStarted( "foo", cluster );

        var someMembers = oneCoreAndOneReadReplica( cluster );

        // when
        restartMembers( someMembers );

        // then
        assertDatabaseEventuallyStarted( "foo", cluster );
    }

    @Test
    void shouldKeepStoppedStateBetweenMemberRestarts() throws Exception
    {
        // given
        var cluster = startCluster();
        createDatabase( "foo", cluster );
        assertDatabaseEventuallyStarted( "foo", cluster );
        stopDatabase( "foo", cluster );
        assertDatabaseEventuallyStopped( "foo", cluster );

        var someMembers = oneCoreAndOneReadReplica( cluster );

        // when
        restartMembers( someMembers );

        // then
        assertDatabaseEventuallyStopped( "foo", cluster );
    }

    @Disabled( "Won't pass until DROP implemented" )
    @Test
    void shouldApplyChangesToCorrectDatabaseIfDropReCreateWhenCoreWasNotConnected() throws Throwable
    {
        // Create database
        var databaseName = "foo";
        var firstLabel = Label.label( "db1" );
        var secondLabel = Label.label( "db2" );
        var timeout = 2;
        var cluster = startCluster();

        cluster.coreTx( SYSTEM_DATABASE_NAME, ( db, tx ) ->
        {
            db.execute( "CREATE DATABASE " + databaseName );
            tx.commit();
        }, timeout, TimeUnit.MINUTES );
        cluster.awaitLeader( databaseName, timeout, TimeUnit.MINUTES );
        cluster.coreTx( GraphDatabaseSettings.DEFAULT_DATABASE_NAME, ( db, tx ) ->
        {
            db.createNode( firstLabel );
            tx.commit();
        }, timeout, TimeUnit.MINUTES  );

        // Stop a core
        var toStop = cluster.awaitLeader( databaseName );
        toStop.shutdown();

        // Drop and recreate database
        cluster.awaitLeader( databaseName );
        cluster.coreTx( SYSTEM_DATABASE_NAME, ( db, tx ) ->
        {
            db.execute( "DROP DATABASE " + databaseName );
            tx.commit();
        }, timeout, TimeUnit.MINUTES );
        cluster.coreTx( SYSTEM_DATABASE_NAME, ( db, tx ) ->
        {
            db.execute( "CREATE DATABASE " + databaseName );
            tx.commit();
        }, timeout, TimeUnit.MINUTES );
        cluster.coreTx( databaseName, ( db, tx ) ->
        {
            db.createNode( secondLabel );
            tx.commit();
        } );

        // Restart core
        toStop.start();

        // Core database should have data only from recreated database
        Assert.assertEventually( () -> hasNodeCount( toStop, databaseName, secondLabel ), equalTo( 1 ), 90, TimeUnit.SECONDS );
        assertThat( hasNodeCount( toStop, databaseName, firstLabel ), equalTo( 0 ) );
    }

    private static Integer hasNodeCount( CoreClusterMember member, String databaseName, Label label )
    {
        var db = member.defaultDatabase();
        assertThat( db.databaseName(), equalTo( databaseName ) );

        try ( var tx = db.beginTransaction( KernelTransaction.Type.explicit, CommercialSecurityContext.AUTH_DISABLED ) )
        {
            var field = "count";
            var result = db.execute( String.format( "MATCH (n:%s) RETURN count(n) AS %s", label, field ) );
            return (Integer)result.next().get( field );
        }
    }

    private static void assertCanStopStartDatabase( String databaseName, Cluster cluster ) throws Exception
    {
        // when
        stopDatabase( databaseName, cluster );

        // then
        assertDatabaseEventuallyStopped( databaseName, cluster );

        // when
        startDatabase( databaseName, cluster );

        // then
        assertDatabaseEventuallyStarted( databaseName, cluster );
    }

    private static void restartMembers( Set<ClusterMember> restartingMembers )
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

    private static void restartCluster( Cluster cluster ) throws InterruptedException, ExecutionException
    {
        cluster.shutdown();
        cluster.start();

        assertDefaultDatabasesAreAvailable( cluster );
    }

    private static void assertDefaultDatabasesAreAvailable( Cluster cluster ) throws InterruptedException
    {
        assertDatabaseEventuallyStarted( SYSTEM_DATABASE_NAME, cluster );
        assertDatabaseEventuallyStarted( DEFAULT_DATABASE_NAME, cluster );
    }

    private static Set<ClusterMember> oneCoreAndOneReadReplica( Cluster cluster )
    {
        return asSet( cluster.getCoreMemberById( 0 ), cluster.getReadReplicaById( 0 ) );
    }
}
