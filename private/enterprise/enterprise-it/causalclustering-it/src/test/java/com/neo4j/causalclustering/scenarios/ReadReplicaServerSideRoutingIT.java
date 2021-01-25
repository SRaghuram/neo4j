/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
@DriverExtension
@TestInstance( PER_METHOD )
class ReadReplicaServerSideRoutingIT
{
    private static final int NR_CORE_MEMBERS = 3;
    private static final int NR_READ_REPLICAS = 1;

    @Inject
    private DriverFactory driverFactory;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster startClusterWithServerSideRoutingConfig( boolean serverSideRoutingEnabled ) throws Exception
    {
        return startCluster( serverSideRoutingConfig( serverSideRoutingEnabled ) );
    }

    @ParameterizedTest()
    @ValueSource( booleans = {true, false} )
    void shouldBeAbleToWriteToReadReplicaWhenServerSideRoutingEnabled( boolean serverSideRoutingEnabled ) throws Exception
    {
        // given
        var cluster = startClusterWithServerSideRoutingConfig( serverSideRoutingEnabled );
        var readReplica = cluster.findAnyReadReplica();

        // when doing write transaction
        if ( serverSideRoutingEnabled )
        {
            createNodeUsingWriteTransaction( readReplica );
            createNodeUsingRun( readReplica );
        }
        else
        {
            var exc = assertThrows( SessionExpiredException.class, () -> createNodeUsingWriteTransaction( readReplica ) );
            assertThat( exc ).hasMessageContaining( "Failed to obtain connection towards WRITE server." );
        }

        // when doing read transaction
        var exc = assertThrows( ClientException.class, () -> createNodeUsingReadTransaction( readReplica ) );
        assertThat( exc ).hasMessageContaining( "Writing in read access mode not allowed" );

        assertReadReplicasEventuallyUpToDateWithLeader( cluster );
        assertThat( countNodes( readReplica ) ).isEqualTo( serverSideRoutingEnabled ? 2 : 0 );
    }

    private void createNodeUsingRun( ReadReplica readReplica ) throws IOException
    {
        try (
                var driver = driverFactory.graphDatabaseDriver( readReplica.neo4jURI() );
                var session = driver.session();
        )
        {
            var newNodeId = session.run( "CREATE (n) RETURN id(n)" ).single().get( 0 ).asInt();
        }
    }

    private void createNodeUsingReadTransaction( ReadReplica readReplica ) throws IOException
    {
        try (
                var driver = driverFactory.graphDatabaseDriver( readReplica.neo4jURI() );
                var session = driver.session();
        )
        {
            var newNodeId = session.readTransaction( tx -> tx.run( "CREATE (n) RETURN id(n)" ).single().get( 0 ).asInt() );
        }
    }

    private void createNodeUsingWriteTransaction( ReadReplica readReplica ) throws IOException
    {
        try (
                var driver = driverFactory.graphDatabaseDriver( readReplica.neo4jURI() );
                var session = driver.session();
        )
        {
            var newNodeId = session.writeTransaction( tx -> tx.run( "CREATE (n) RETURN id(n)" ).single().get( 0 ).asInt() );
        }
    }

    private int countNodes( ReadReplica readReplica ) throws IOException
    {
        try (
                var driver = driverFactory.graphDatabaseDriver( readReplica.neo4jURI() );
                var session = driver.session();
        )
        {
            return session.readTransaction( tx -> tx.run( "MATCH (n) RETURN COUNT(n)" ).single().get( 0 ).asInt() );
        }
    }

    private Cluster startCluster( ClusterConfig clusterConfig ) throws Exception
    {
        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        return cluster;
    }

    private static ClusterConfig serverSideRoutingConfig( boolean serverSideRoutingEnabled )
    {
        return clusterConfig()
                .withNumberOfCoreMembers( NR_CORE_MEMBERS )
                .withNumberOfReadReplicas( NR_READ_REPLICAS )
                .withSharedReadReplicaParam( GraphDatabaseSettings.routing_enabled, serverSideRoutingEnabled ? "true" : "false" )
                .withSharedCoreParam( GraphDatabaseSettings.routing_enabled, serverSideRoutingEnabled ? "true" : "false" );
    }

    private static void assertReadReplicasEventuallyUpToDateWithLeader( Cluster cluster )
    {
        assertEventually( () -> ReadReplicasProgress.of( cluster ), new HamcrestCondition<>( readReplicasUpToDateWithLeader() ), 1, MINUTES );
    }

    private static long lastClosedTransactionId( boolean fail, GraphDatabaseFacade db )
    {
        try
        {
            return db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
        }
        catch ( Exception e )
        {
            if ( !fail )
            {
                // the db is down we'll try again...
                return -1;
            }
            throw e;
        }
    }

    private static Matcher<ReadReplicasProgress> readReplicasUpToDateWithLeader()
    {
        return new TypeSafeMatcher<>()
        {
            @Override
            protected boolean matchesSafely( ReadReplicasProgress progress )
            {
                return progress.readReplicasUpToDateWithLeader();
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Read replicas up-to-date with the leader" );
            }

            @Override
            protected void describeMismatchSafely( ReadReplicasProgress progress, Description description )
            {
                description.appendText( "Leader's last committed transaction ID: " ).appendValue( progress.leaderLastClosedTxId ).appendText( "\n" )
                           .appendText( "Read replicas last committed transaction IDs: " ).appendValue( progress.readReplicaLastClosedTxIds );
            }
        };
    }

    private static class ReadReplicasProgress
    {
        final long leaderLastClosedTxId;
        final Map<ReadReplica,Long> readReplicaLastClosedTxIds;

        ReadReplicasProgress( long leaderLastClosedTxId, Map<ReadReplica,Long> readReplicaLastClosedTxIds )
        {
            this.leaderLastClosedTxId = leaderLastClosedTxId;
            this.readReplicaLastClosedTxIds = readReplicaLastClosedTxIds;
        }

        static ReadReplicasProgress of( Cluster cluster ) throws TimeoutException
        {
            var leader = cluster.awaitLeader();
            var leaderLastClosedTxId = lastClosedTransactionId( true, leader.defaultDatabase() );

            var readReplicaLastClosedTxIds = cluster.readReplicas()
                                                    .stream()
                                                    .collect( toMap( identity(),
                                                                     readReplica -> lastClosedTransactionId( false, readReplica.defaultDatabase() ) ) );

            return new ReadReplicasProgress( leaderLastClosedTxId, readReplicaLastClosedTxIds );
        }

        boolean readReplicasUpToDateWithLeader()
        {
            return readReplicaLastClosedTxIds.values()
                                             .stream()
                                             .allMatch( readReplicaLastClosedTxId -> readReplicaLastClosedTxId == leaderLastClosedTxId );
        }
    }
}
