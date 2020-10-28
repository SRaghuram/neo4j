/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.causalclustering.readreplica.CatchupProcessManager;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.forceReelection;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.runWithLeaderDisabled;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_topology_refresh;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

@DriverExtension
class BoltCausalClusteringIT
{
    @Inject
    DriverFactory driverFactory;
    private static final long DEFAULT_TIMEOUT_MS = 15_000;

    @Nested
    @ClusterExtension
    class SharedCluster
    {
        @Inject
        private ClusterFactory clusterFactory;

        private Cluster cluster;

        @BeforeAll
        void startCluster() throws Exception
        {
            cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 1 ) );
            cluster.start();
        }

        @BeforeEach
        void removePersons() throws TimeoutException, IOException
        {
            try ( Driver driver = makeDriver( cluster ) )
            {
                // when
                inExpirableSession( driver, Driver::session, session -> session.run( "MATCH (n:Person) DELETE n" ).consume() );
            }
        }

        @Test
        void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfLeader() throws Exception
        {
            //given
            cluster.coreTx( ( db, tx ) ->
            {
                Iterators.count( tx.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" ) );
                tx.commit();
            } );

            // when
            int count = executeWriteAndReadThroughBolt( cluster.awaitLeader() );

            // then
            assertEquals( 1, count );

            cluster.coreTx( ( db, tx ) ->
            {
                Iterators.count( tx.execute( "DROP CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" ) );
                tx.commit();
            } );
        }

        @Test
        void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfFollower() throws Exception
        {
            //given
            cluster.coreTx( ( db, tx ) ->
            {
                Iterators.count( tx.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" ) );
                tx.commit();
            } );

            // when
            int count = executeWriteAndReadThroughBolt( cluster.awaitCoreMemberWithRole( Role.FOLLOWER ) );

            // then
            assertEquals( 1, count );

            cluster.coreTx( ( db, tx ) ->
            {
                Iterators.count( tx.execute( "DROP CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" ) );
                tx.commit();
            } );
        }

        @Test
        void shouldNotBeAbleToWriteOnAReadSession() throws Exception
        {
            runWithLeaderDisabled( cluster, DEFAULT_DATABASE_NAME, ( oldLeader, otherMembers ) ->
            {
                try ( Driver driver = makeDriver( oldLeader.directURI() ); Session session = driver.session( builder().withDefaultAccessMode( READ ).build() ) )
                {
                    var ex = assertThrows( ClientException.class, () -> session.run( "CREATE (n:Person {name: 'Jim'})" ).consume() );
                    assertEquals( "Writing in read access mode not allowed. Attempted write to internal graph 0 (neo4j)", ex.getMessage() );
                    return null;
                }
            } );
        }

        @Test
        void shouldPickANewServerToWriteToOnLeaderSwitch() throws Throwable
        {
            String firstAddress;
            try ( Driver driver = makeDriver( cluster ) )
            {
                firstAddress = inExpirableSession( driver, Driver::session, session ->
                {
                    Result result = session.run( "CREATE (p:Person)" );
                    return result.consume().server().address();
                } );

                var secondAddress = runWithLeaderDisabled( cluster, ( oldLeader, currentMembers ) -> inExpirableSession( driver, Driver::session, session ->
                {
                    Result result = session.run( "CREATE (p:Person)" );
                    return result.consume().server().address();
                }, 60_000 ) );
                assertNotEquals( secondAddress, firstAddress );
            }
        }

        @Test
        void sessionShouldExpireOnLeaderSwitch() throws Exception
        {
            try ( Driver driver = makeDriver( cluster );
                  Session session = driver.session() )
            {
                session.run( "CREATE (n:Person {name: 'Jim'})" ).consume();

                // when
                runWithLeaderDisabled( cluster, ( oldLeader, otherMembers ) ->
                {
                    Exception ex = assertThrows( Exception.class, () -> session.run( "CREATE (n:Person {name: 'Mark'})" ).consume() );
                    assertThat( ex, either( instanceOf( SessionExpiredException.class ) ).or( instanceOf( TransientException.class ) ) );
                    return null;
                } );
            }
        }

        @Test
        void shouldBeAbleToGetClusterOverview() throws Exception
        {
            // given
            int clusterSize = cluster.allMembers().size();

            try ( Driver driver = makeDriver( cluster );
                  Session session = driver.session() )
            {
                assertEventually( () -> session.run( "CALL dbms.cluster.overview" ).list(), new HamcrestCondition<>( hasSize( clusterSize ) ),
                        60, SECONDS );
            }
        }

        /*
           Create a session with empty arg list (no AccessMode arg), in a driver that was initialized with a bolt+routing
           URI, and ensure that it
           a) works against the Leader for reads and writes before a leader switch, and
           b) receives a SESSION EXPIRED after a leader switch, and
           c) keeps working if a new session is created after that exception, again with no access mode specified.
         */
        @Test
        void shouldReadAndWriteToANewSessionCreatedAfterALeaderSwitch() throws Exception
        {
            try ( Driver driver = makeDriver( cluster ) )
            {
                try ( Session session = driver.session() )
                {
                    // execute a write/read query
                    var record = session.writeTransaction( tx ->
                                                           {
                                                               tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Jim" ) ).consume();
                                                               return tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).single();
                                                           } );
                    assertEquals( 1, record.get( "count" ).asInt() );

                    // change leader

                    runWithLeaderDisabled( cluster, ( oldLeader, otherMembers ) ->
                    {
                        var ex = assertThrows( Exception.class, () -> session
                                .run( "CREATE (p:Person {name: $name })", parameters( "name", "Mark" ) ).consume() );
                        assertThat( ex, either( instanceOf( SessionExpiredException.class ) ).or( instanceOf( TransientException.class ) ) );
                        return null;
                    } );
                }

                inExpirableSession( driver, Driver::session, session ->
                {
                    // execute a write/read query
                    session.run( "CREATE (p:Person {name: $name })", parameters( "name", "Jim" ) );
                    Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).single();
                    assertEquals( 2, record.get( "count" ).asInt() );
                    return null;
                } );
            }
        }

        // Ensure that Bookmarks work with single instances using a driver created using a bolt[not+routing] URI.
        @Test
        void bookmarksShouldWorkWithDriverPinnedToSingleServer() throws Exception
        {
            try ( Driver driver = makeDriver( cluster ) )
            {
                Bookmark bookmark = inExpirableSession( driver, Driver::session, session ->
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Alistair" ) );
                        tx.commit();
                    }

                    return session.lastBookmark();
                } );

                assertNotNull( bookmark );

                try ( Session session = driver.session( builder().withBookmarks( bookmark ).build() );
                      Transaction tx = session.beginTransaction() )
                {
                    Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                    assertEquals( 1, record.get( "count" ).asInt() );
                    tx.commit();
                }
            }
        }

        @Test
        void shouldUseBookmarkFromAReadSessionInAWriteSession() throws Exception
        {
            try ( Driver driver = makeDriver( cluster ) )
            {
                inExpirableSession( driver, Driver::session, session ->
                {
                    session.run( "CREATE (p:Person {name: $name })", parameters( "name", "Jim" ) );
                    return null;
                } );

                Bookmark bookmark;
                try ( Session session = driver.session( builder().withDefaultAccessMode( READ ).build() ) )
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                        tx.commit();
                    }

                    bookmark = session.lastBookmark();
                }

                assertNotNull( bookmark );

                inExpirableSession( driver, d -> d.session( builder().withDefaultAccessMode( WRITE ).withBookmarks( bookmark ).build() ), session ->
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Alistair" ) );
                        tx.commit();
                    }

                    return null;
                } );

                try ( Session session = driver.session() )
                {
                    Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                    assertEquals( 2, record.get( "count" ).asInt() );
                }
            }
        }

        @Test
        void shouldUseBookmarkFromAWriteSessionInAReadSession() throws Throwable
        {
            // given
            ReadReplica readReplica = cluster.getReadReplicaByIndex( 0 );

            CatchupProcessManager catchupProcessManager = readReplica.resolveDependency( DEFAULT_DATABASE_NAME, CatchupProcessManager.class );
            catchupProcessManager.stop();

            try ( Driver driver1 = makeDriver( cluster ) )
            {
                Bookmark bookmark = inExpirableSession( driver1, Driver::session, session ->
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Jim" ) );
                        tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Alistair" ) );
                        tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Mark" ) );
                        tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Chris" ) );
                        tx.commit();
                    }

                    return session.lastBookmark();
                } );

                assertNotNull( bookmark );
                catchupProcessManager.start();

                try ( Driver driver2 = makeDriver( readReplica.directURI() ) )
                {
                    try ( Session session = driver2.session( builder().withDefaultAccessMode( READ ).withBookmarks( bookmark ).build() ) )
                    {
                        try ( Transaction tx = session.beginTransaction() )
                        {
                            Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                            assertEquals( 4, record.get( "count" ).asInt() );
                            tx.commit();
                        }
                    }
                }
            }
        }

        @Test
        void shouldHandleLeaderSwitch() throws Exception
        {
            try ( Driver driver = makeDriver( cluster ) )
            {
                // when
                try ( Session session = driver.session() )
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        forceReelection( cluster, DEFAULT_DATABASE_NAME );

                        tx.run( "CREATE (person:Person {name: $name, title: $title})", parameters( "name", "Webber", "title", "Mr" ) );
                        tx.commit();
                    }
                    catch ( SessionExpiredException | TransientException ignored )
                    {
                        // expected
                    }
                }

                Bookmark bookmark = inExpirableSession( driver, Driver::session, s ->
                {
                    try ( Transaction tx = s.beginTransaction() )
                    {
                        tx.run( "CREATE (person:Person {name: $name, title: $title})", parameters( "name", "Webber", "title", "Mr" ) );
                        tx.commit();
                    }
                    return s.lastBookmark();
                } );

                // then
                try ( Session session = driver.session( builder().withDefaultAccessMode( READ ).withBookmarks( bookmark ).build() ) )
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                        assertEquals( 1, record.get( "count" ).asInt() );
                        tx.commit();
                    }
                }
            }
        }
    }

    @Nested
    @ClusterExtension
    @TestInstance( TestInstance.Lifecycle.PER_METHOD )
    class PerTestCluster
    {

        @Inject
        ClusterFactory clusterFactory;

        @Test
        void shouldSendRequestsToNewlyAddedReadReplicas() throws Throwable
        {
            // given
            var cluster = clusterFactory.createCluster( ClusterConfig
                    .clusterConfig()
                    .withNumberOfCoreMembers( 3 )
                    .withNumberOfReadReplicas( 1 )
                    .withSharedCoreParam( cluster_topology_refresh, "5s" )
                    .withSharedReadReplicaParam( cluster_topology_refresh, "5s" )
                    .withSharedCoreParam( cluster_allow_reads_on_followers, "false" )
                    .withSharedReadReplicaParam( GraphDatabaseSettings.bookmark_ready_timeout, "1s" )
                    .withSharedCoreParam( GraphDatabaseSettings.routing_ttl, "1s" ) );
            cluster.start();

            try ( Driver driver = makeDriver( cluster ) )
            {

                Bookmark bookmark = inExpirableSession( driver, Driver::session, session ->
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Jim" ) );
                        tx.commit();
                    }

                    return session.lastBookmark();
                } );

                // when
                Set<String> readReplicas = new HashSet<>();

                for ( ReadReplica readReplica : cluster.readReplicas() )
                {
                    readReplicas.add( readReplica.boltAdvertisedAddress() );
                }

                for ( int i = 10; i <= 13; i++ )
                {
                    ReadReplica newReadReplica = cluster.addReadReplicaWithIndex( i );
                    readReplicas.add( newReadReplica.boltAdvertisedAddress() );
                    newReadReplica.start();
                }

                assertEventually( "Failed to send requests to all servers", () ->
                {
                    for ( int i = 0; i < cluster.readReplicas().size(); i++ ) // don't care about cores
                    {
                        try ( Session session = driver.session( builder().withDefaultAccessMode( READ ).withBookmarks( bookmark ).build() ) )
                        {
                            executeReadQuery( session );

                            session.readTransaction( (TransactionWork<Void>) tx ->
                            {
                                Result result = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" );

                                assertEquals( 1, result.next().get( "count" ).asInt() );

                                readReplicas.remove( result.consume().server().address() );

                                return null;
                            } );
                        }
                        catch ( Throwable throwable )
                        {
                            return false;
                        }
                    }

                    return readReplicas.size() == 0; // have sent something to all replicas
                }, TRUE, 120, SECONDS );
            }
        }

        @Test
        void transactionsShouldNotAppearOnTheReadReplicaWhilePollingIsPaused() throws Throwable
        {
            // given
            Map<String,String> params =
                    Map.of( GraphDatabaseSettings.keep_logical_logs.name(), "keep_none", GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M",
                            GraphDatabaseSettings.check_point_interval_time.name(), "100ms", CausalClusteringSettings.cluster_allow_reads_on_followers.name(),
                            FALSE );

            Cluster cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig().withSharedCoreParams( params ).withNumberOfReadReplicas( 1 ) );
            cluster.start();

            int happyCount;
            int numberOfRequests;
            try ( Driver driver = makeDriver( cluster ) )
            {

                try ( Session session = driver.session() )
                {
                    session.writeTransaction( tx ->
                    {
                        tx.run( "MERGE (n:Person {name: 'Jim'})" );
                        return null;
                    } );
                }

                ReadReplica replica = cluster.findAnyReadReplica();

                CatchupProcessManager catchupProcessManager = replica.defaultDatabase().getDependencyResolver()
                        .resolveDependency( CatchupProcessManager.class );

                catchupProcessManager.stop();

                Bookmark lastBookmark = null;
                int iterations = 5;
                final int nodesToCreate = 20000;
                for ( int i = 0; i < iterations; i++ )
                {
                    try ( Session writeSession = driver.session() )
                    {
                        writeSession.writeTransaction( tx ->
                        {

                            tx.run( "UNWIND range(1, $nodesToCreate) AS i CREATE (n:Person {name: 'Jim'})", parameters( "nodesToCreate", nodesToCreate ) );
                            return null;
                        } );

                        lastBookmark = writeSession.lastBookmark();
                    }
                }
                try ( Session session = driver.session( builder().withBookmarks( lastBookmark ).withDefaultAccessMode( READ ).build() ) )
                {
                    var transientException = assertThrows( TransientException.class, () -> session.run( "RETURN 1" ).consume() );
                    assertThat( transientException.getMessage(), containsString( "not up to the requested version:" ) );
                }

                // when the poller is resumed, it does make it to the read replica
                catchupProcessManager.start();

                catchupProcessManager.getCatchupProcess().upToDateFuture().get();

                happyCount = 0;
                numberOfRequests = 1_000;
                Bookmark bookmark = lastBookmark;
                for ( int i = 0; i < numberOfRequests; i++ ) // don't care about cores
                {
                    try ( Session session = driver.session( builder().withBookmarks( bookmark ).build() ) )
                    {
                        happyCount += session.readTransaction( tx ->
                        {
                            tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" );
                            return 1;
                        } );
                    }
                }
            }

            assertEquals( numberOfRequests, happyCount );
        }
    }

    private int executeWriteAndReadThroughBolt( CoreClusterMember core ) throws TimeoutException, IOException
    {
        try ( Driver driver = makeDriver( core.routingURI() ) )
        {

            return inExpirableSession( driver, Driver::session, session ->
            {
                // when
                session.run( "MERGE (n:Person {name: 'Jim'})" ).consume();
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                return record.get( "count" ).asInt();
            } );
        }
    }

    private Driver makeDriver( Cluster cluster ) throws IOException
    {
        return driverFactory.graphDatabaseDriver( cluster );
    }

    private Driver makeDriver( String uri ) throws IOException
    {
        return driverFactory.graphDatabaseDriver( uri );
    }

    private static void executeReadQuery( Session session )
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
            assertEquals( 1, record.get( "count" ).asInt() );
        }
    }

    private static <T> T inExpirableSession( Driver driver, Function<Driver,Session> acquirer, Function<Session,T> op ) throws TimeoutException
    {
        return inExpirableSession( driver, acquirer, op, DEFAULT_TIMEOUT_MS );
    }

    private static <T> T inExpirableSession( Driver driver, Function<Driver,Session> acquirer, Function<Session,T> op, long timeoutMs ) throws TimeoutException
    {
        long endTime = System.currentTimeMillis() + timeoutMs;

        do
        {
            try ( Session session = acquirer.apply( driver ) )
            {
                return op.apply( session );
            }
            catch ( SessionExpiredException | TransientException | ClientException e )
            {
                // role might have changed; try again;
            }
        }
        while ( System.currentTimeMillis() < endTime );

        throw new TimeoutException( "Transaction did not succeed in time" );
    }
}
