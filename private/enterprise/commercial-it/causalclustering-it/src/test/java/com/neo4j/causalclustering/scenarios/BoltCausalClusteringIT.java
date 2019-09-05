/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.causalclustering.readreplica.CatchupPollingProcess;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.summary.ServerInfo;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.SessionConfig.builder;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
@ExtendWith( SuppressOutputExtension.class )
class BoltCausalClusteringIT
{
    private static final long DEFAULT_TIMEOUT_MS = 15_000;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void startCluster() throws Exception
    {
        cluster = clusterFactory.createCluster( ClusterConfig
                .clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withSharedCoreParams( stringMap( GraphDatabaseSettings.routing_ttl.name(), "3s" ) ) );
        cluster.start();
    }

    @AfterAll
    void shutdownCluster()
    {
        cluster.shutdown();
    }

    @BeforeEach
    void removePersons() throws TimeoutException
    {
        try ( Driver driver = makeDriver( cluster.awaitLeader().routingURI() );
                Session session = driver.session( builder().withDefaultAccessMode( WRITE ).build() ) )
        {
            // when
            session.run( "MATCH (n:Person) DELETE n" ).consume();
        }
    }

    private static Driver makeDriver( String uri )
    {
        return graphDatabaseDriver( uri, AuthTokens.basic( "neo4j", "neo4j" ) );
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

    private static int executeWriteAndReadThroughBolt( CoreClusterMember core ) throws TimeoutException
    {
        try ( Driver driver = makeDriver( core.routingURI() ) )
        {

            return inExpirableSession( driver, d -> d.session( builder().withDefaultAccessMode( WRITE ).build() ), session ->
            {
                // when
                session.run( "MERGE (n:Person {name: 'Jim'})" ).consume();
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                return record.get( "count" ).asInt();
            } );
        }
    }

    @Test
    void shouldNotBeAbleToWriteOnAReadSession() throws Exception
    {
        // given
        assertEventually( "Failed to execute write query on read server", () ->
        {
            switchLeader( cluster.awaitLeader() );
            CoreClusterMember leader = cluster.awaitLeader();

            try ( Driver driver = makeDriver( leader.routingURI() );
                    Session session = driver.session( builder().withDefaultAccessMode( READ ).build() ) )
            {
                // when
                session.run( "CREATE (n:Person {name: 'Jim'})" ).consume();
                return false;
            }
            catch ( ClientException ex )
            {
                assertEquals( "Write queries cannot be performed in READ access mode.", ex.getMessage() );
                return true;
            }
        }, is( true ), 30, SECONDS );
    }

    @Test
    void sessionShouldExpireOnLeaderSwitch() throws Exception
    {
        // given
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = makeDriver( leader.routingURI() );
                Session session = driver.session() )
        {
            session.run( "CREATE (n:Person {name: 'Jim'})" ).consume();

            // when
            switchLeader( leader );

            SessionExpiredException sep =
                    Assertions.assertThrows( SessionExpiredException.class, () -> session.run( "CREATE (n:Person {name: 'Mark'})" ).consume() );
            assertThat( sep.getMessage(), containsString( "no longer accepts writes" ) );
        }
    }

    @Test
    void shouldBeAbleToGetClusterOverview() throws Exception
    {
        // given
        CoreClusterMember leader = cluster.awaitLeader();

        int clusterSize = cluster.readReplicas().size() + cluster.coreMembers().size();

        try ( Driver driver = makeDriver( leader.routingURI() );
              Session session = driver.session() )
        {
            assertEventually( () -> session.run( "CALL dbms.cluster.overview" ).list(), hasSize( clusterSize ), 60, SECONDS );
        }
    }

    /**
     * Keeps the leader different than the initial leader.
     */
    private class LeaderSwitcher implements Runnable
    {
        private final Cluster cluster;
        private final CountDownLatch switchCompleteLatch;
        private CoreClusterMember initialLeader;
        private CoreClusterMember currentLeader;

        private Thread thread;
        private boolean stopped;
        private Throwable throwable;

        LeaderSwitcher( Cluster cluster, CountDownLatch switchCompleteLatch )
        {
            this.cluster = cluster;
            this.switchCompleteLatch = switchCompleteLatch;
        }

        @Override
        public void run()
        {
            try
            {
                initialLeader = cluster.awaitLeader();

                while ( !stopped )
                {
                    currentLeader = cluster.awaitLeader();
                    if ( currentLeader == initialLeader )
                    {
                        switchLeader( initialLeader );
                        currentLeader = cluster.awaitLeader();
                    }
                    else
                    {
                        switchCompleteLatch.countDown();
                    }

                    Thread.sleep( 100 );
                }
            }
            catch ( Throwable e )
            {
                throwable = e;
            }
        }

        void start()
        {
            if ( thread == null )
            {
                thread = new Thread( this );
                thread.start();
            }
        }

        void stop() throws Throwable
        {
            if ( thread != null )
            {
                stopped = true;
                thread.join();
            }

            assertNoException();
        }

        boolean hadLeaderSwitch()
        {
            return currentLeader != initialLeader;
        }

        void assertNoException() throws Throwable
        {
            if ( throwable != null )
            {
                throw throwable;
            }
        }
    }

    @Test
    void shouldPickANewServerToWriteToOnLeaderSwitch() throws Throwable
    {
        // given
        CoreClusterMember leader = cluster.awaitLeader();

        CountDownLatch leaderSwitchLatch = new CountDownLatch( 1 );

        LeaderSwitcher leaderSwitcher = new LeaderSwitcher( cluster, leaderSwitchLatch );

        Set<String> seenAddresses = new HashSet<>();
        try ( Driver driver = makeDriver( leader.routingURI() ) )
        {
            boolean success = false;

            long deadline = System.currentTimeMillis() + (30 * 1000);

            while ( !success )
            {
                if ( System.currentTimeMillis() > deadline )
                {
                    fail( "Failed to write to the new leader in time. Addresses seen: " + seenAddresses );
                }

                try ( Session session = driver.session( builder().withDefaultAccessMode( WRITE ).build() ) )
                {
                    StatementResult result = session.run( "CREATE (p:Person)" );
                    ServerInfo server = result.summary().server();
                    seenAddresses.add( server.address());
                    success = seenAddresses.size() >= 2;
                }
                catch ( Exception e )
                {
                    Thread.sleep( 100 );
                }

                /*
                 * Having the latch release here ensures that we've done at least one pass through the loop, which means
                 * we've completed a connection before the forced master switch.
                 */
                if ( !seenAddresses.isEmpty() && !success )
                {
                    leaderSwitcher.start();
                    leaderSwitchLatch.await();
                }
            }
        }
        finally
        {
            leaderSwitcher.stop();
            assertTrue( leaderSwitcher.hadLeaderSwitch() );
            assertThat( seenAddresses.size(), greaterThanOrEqualTo( 2 ) );
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
        // given
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = makeDriver( leader.routingURI() ) )
        {
            inExpirableSession( driver, Driver::session, session ->
            {
                // execute a write/read query
                session.run( "CREATE (p:Person {name: $name })", parameters( "name", "Jim" ) );
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).single();
                assertEquals( 1, record.get( "count" ).asInt() );

                // change leader

                try
                {
                    switchLeader( leader );
                    session.run( "CREATE (p:Person {name: $name })", parameters( "name", "Mark" ) ).consume();
                    fail( "Should have thrown an exception as the leader went away mid session" );
                }
                catch ( SessionExpiredException sep )
                {
                    assertThat( sep.getMessage(), containsString( "no longer accepts writes" ) );
                }
                catch ( InterruptedException e )
                {
                    // ignored
                }
                return null;
            } );

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
        // given
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = makeDriver( leader.directURI() ) )
        {
            String bookmark = inExpirableSession( driver, Driver::session, session ->
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Alistair" ) );
                    tx.success();
                }

                return session.lastBookmark();
            } );

            assertNotNull( bookmark );

            try ( Session session = driver.session( builder().withBookmarks( bookmark ).build() );
                    Transaction tx = session.beginTransaction() )
            {
                Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 1, record.get( "count" ).asInt() );
                tx.success();
            }
        }
    }

    @Test
    void shouldUseBookmarkFromAReadSessionInAWriteSession() throws Exception
    {
        // given
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = makeDriver( leader.directURI() ) )
        {
            inExpirableSession( driver, d -> d.session( builder().withDefaultAccessMode( WRITE ).build() ), session ->
            {
                session.run( "CREATE (p:Person {name: $name })", parameters( "name", "Jim" ) );
                return null;
            } );

            String bookmark;
            try ( Session session = driver.session( builder().withDefaultAccessMode( READ ).build() ) )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                    tx.success();
                }

                bookmark = session.lastBookmark();
            }

            assertNotNull( bookmark );

            inExpirableSession( driver, d -> d.session( builder().withDefaultAccessMode( WRITE ).withBookmarks( bookmark ).build() ), session ->
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Alistair" ) );
                    tx.success();
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
        CoreClusterMember leader = cluster.awaitLeader();
        ReadReplica readReplica = cluster.getReadReplicaById( 0 );

        CatchupPollingProcess catchupPollingProcess = readReplica.resolveDependency( DEFAULT_DATABASE_NAME, CatchupPollingProcess.class );
        catchupPollingProcess.stop();

        try ( Driver driver1 = makeDriver( leader.directURI() ) )
        {

            String bookmark = inExpirableSession( driver1, d -> d.session( builder().withDefaultAccessMode( WRITE ).build() ), session ->
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Jim" ) );
                    tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Alistair" ) );
                    tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Mark" ) );
                    tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Chris" ) );
                    tx.success();
                }

                return session.lastBookmark();
            } );

            assertNotNull( bookmark );
            catchupPollingProcess.start();

            try ( Driver driver2 = makeDriver( readReplica.directURI() ) )
            {

                try ( Session session = driver2.session( builder().withDefaultAccessMode( READ ).withBookmarks( bookmark ).build() ) )
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                        tx.success();
                        assertEquals( 4, record.get( "count" ).asInt() );
                    }
                }
            }
        }
    }

    @Test
    void shouldSendRequestsToNewlyAddedReadReplicas() throws Throwable
    {
        // given
        CoreClusterMember leader = cluster.awaitLeader();
        try ( Driver driver = makeDriver( leader.routingURI() ) )
        {

            String bookmark = inExpirableSession( driver, d -> d.session( builder().withDefaultAccessMode( WRITE ).build() ), session ->
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (p:Person {name: $name })", parameters( "name", "Jim" ) );
                    tx.success();
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
                ReadReplica newReadReplica = cluster.addReadReplicaWithId( i );
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
                            StatementResult result = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" );

                            assertEquals( 1, result.next().get( "count" ).asInt() );

                            readReplicas.remove( result.summary().server().address() );

                            return null;
                        } );
                    }
                    catch ( Throwable throwable )
                    {
                        return false;
                    }
                }

                return readReplicas.size() == 0; // have sent something to all replicas
            }, is( true ), 30, SECONDS );
        }
    }

    @Test
    void shouldHandleLeaderSwitch() throws Exception
    {
        // given
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = makeDriver( leader.routingURI() ) )
        {
            // when
            try ( Session session = driver.session() )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    switchLeader( leader );

                    tx.run( "CREATE (person:Person {name: $name, title: $title})", parameters( "name", "Webber", "title", "Mr" ) );
                    tx.success();
                }
                catch ( SessionExpiredException ignored )
                {
                    // expected
                }
            }

            String bookmark = inExpirableSession( driver, Driver::session, s ->
            {
                try ( Transaction tx = s.beginTransaction() )
                {
                    tx.run( "CREATE (person:Person {name: $name, title: $title})", parameters( "name", "Webber", "title", "Mr" ) );
                    tx.success();
                }
                return s.lastBookmark();
            } );

            // then
            try ( Session session = driver.session( builder().withDefaultAccessMode( READ ).withBookmarks( bookmark ).build() ) )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                    tx.success();
                    assertEquals( 1, record.get( "count" ).asInt() );
                }
            }
        }
    }

    @Test
    void transactionsShouldNotAppearOnTheReadReplicaWhilePollingIsPaused() throws Throwable
    {
        // given
        Map<String,String> params =
                stringMap( GraphDatabaseSettings.keep_logical_logs.name(), "keep_none", GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M",
                        GraphDatabaseSettings.check_point_interval_time.name(), "100ms", CausalClusteringSettings.cluster_allow_reads_on_followers.name(),
                        FALSE );

        Cluster cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig().withSharedCoreParams( params ).withNumberOfReadReplicas( 1 ) );
        cluster.start();

        int happyCount;
        int numberOfRequests;
        try ( Driver driver = makeDriver( cluster.awaitLeader().routingURI() ) )
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

            CatchupPollingProcess pollingClient = replica.defaultDatabase().getDependencyResolver().resolveDependency( CatchupPollingProcess.class );

            pollingClient.stop();

            String lastBookmark = null;
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

            // when the poller is resumed, it does make it to the read replica
            pollingClient.start();

            pollingClient.upToDateFuture().get();

            happyCount = 0;
            numberOfRequests = 1_000;
            String bookmark = lastBookmark;
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

        cluster.shutdown();
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
        long endTime = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        do
        {
            try ( Session session = acquirer.apply( driver ) )
            {
                return op.apply( session );
            }
            catch ( SessionExpiredException | ClientException e )
            {
                // role might have changed; try again;
            }
        }
        while ( System.currentTimeMillis() < endTime );

        throw new TimeoutException( "Transaction did not succeed in time" );
    }

    private void switchLeader( CoreClusterMember initialLeader ) throws InterruptedException
    {
        long deadline = System.currentTimeMillis() + (30 * 1000);

        Role role = getCurrentCoreRole( initialLeader.defaultDatabase() );
        while ( role != Role.FOLLOWER )
        {
            if ( System.currentTimeMillis() > deadline )
            {
                throw new RuntimeException( "Failed to switch leader in time" );
            }

            try
            {
                triggerElection( initialLeader );
            }
            catch ( IOException | TimeoutException e )
            {
                // keep trying
            }
            finally
            {
                role = getCurrentCoreRole( initialLeader.defaultDatabase() );
                Thread.sleep( 100 );
            }
        }
    }

    private static Role getCurrentCoreRole( GraphDatabaseFacade database )
    {
        return database.getDependencyResolver().resolveDependency( RoleProvider.class ).currentRole();
    }

    private void triggerElection( CoreClusterMember initialLeader ) throws IOException, TimeoutException
    {
        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            if ( !coreClusterMember.equals( initialLeader ) )
            {
                RaftMachine raft = coreClusterMember.resolveDependency( DEFAULT_DATABASE_NAME, RaftMachine.class );
                raft.triggerElection();
                cluster.awaitLeader();
            }
        }
    }
}
