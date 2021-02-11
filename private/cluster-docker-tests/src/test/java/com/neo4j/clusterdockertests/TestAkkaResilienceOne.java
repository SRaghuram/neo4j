/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.driver.ClusterChecker;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Record;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.CoreModifier;
import org.neo4j.junit.jupiter.causal_cluster.NeedsCausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jServer;
import org.neo4j.logging.Level;
import org.neo4j.test.extension.Inject;

import static com.neo4j.configuration.MetricsSettings.metrics_filter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.assertion.Assert.assertEventuallyDoesNotThrow;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@NeedsCausalCluster
@DriverExtension
@ExtendWith( DumpDockerLogs.class )
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@TestMethodOrder( MethodOrderer.OrderAnnotation.class )
public class TestAkkaResilienceOne
{
    private static final String akkaRestartMessage = "Restarting discovery system after probable network partition";
    private static final String downingMessage = "Leader is removing unreachable node";
    private static final String nodeJoiningItselfMessage = "is JOINING itself";

    private static final Duration detectFailureRestartAkkaAndReformTimeout = Duration.ofSeconds( 90 );
    private static final Duration acceptableHeartbeatPause = Duration.ofSeconds( 30 );

    private static final AuthToken authToken = AuthTokens.basic( "neo4j", "password" );

    @CausalCluster
    private static Neo4jCluster cluster;

    private final Logger log = LoggerFactory.getLogger( this.getClass() );

    @Inject
    private DriverFactory driverFactory;

    private ClusterChecker clusterChecker;
    private int clusterSize;

    @CoreModifier
    private static Neo4jContainer<?> configure( Neo4jContainer<?> input ) throws IOException
    {
        // The settings here make the akka failure detector behave more like a fixed timeout failure detector.
        // A large heartbeat pause makes the test take longer to run but prevents flakiness because of contention or GC or VM pauses when running in TC
        List<GlobbingPattern> metricsFilter = metrics_filter.defaultValue();
        metricsFilter.add( new GlobbingPattern( "neo4j.causal_clustering.*" ) );
        String metricsFilterString = metricsFilter.stream().map( GlobbingPattern::toString ).collect( Collectors.joining( "," ) );

        return DeveloperWorkflow.configureNeo4jContainerIfNecessary( input )
                                .withNeo4jConfig( metrics_filter.name(), metricsFilterString )
                                .withNeo4jConfig( CausalClusteringSettings.middleware_logging_level.name(), Level.DEBUG.toString() )
                                .withNeo4jConfig( CausalClusteringInternalSettings.akka_failure_detector_acceptable_heartbeat_pause.name(),
                                                  acceptableHeartbeatPause.toSeconds() + "s" )
                                .withNeo4jConfig( CausalClusteringInternalSettings.akka_failure_detector_expected_response_after.name(), "10s" )
                                .withNeo4jConfig( CausalClusteringInternalSettings.akka_failure_detector_threshold.name(), "4" );
    }

    @BeforeAll
    void setUp()
    {
        clusterSize = cluster.getAllServers().size();
        driverFactory.setAuthToken( authToken );
    }

    @AfterAll
    void tearDown() throws Exception
    {
    }

    @BeforeEach
    void before() throws TimeoutException, Neo4jCluster.Neo4jTimeoutException, IOException
    {
        clusterChecker = driverFactory.clusterChecker(
                cluster.getAllServers().stream().map( Neo4jServer::getDirectBoltUri ).collect( Collectors.toList() )
        );
        assertThat( clusterChecker.size() ).isEqualTo( clusterSize );

        try
        {
            cluster.waitForLogMessageOnAll( cluster.getAllServers(), "Started.", Duration.ofMinutes( 2 ) );
            clusterChecker.verifyConnectivity();
            checkClusterState();
        }
        catch ( Throwable e )
        {
            log.warn( () -> "Before test: cluster not usable: " + e.getMessage() + "\nAttempting to recover..." );
            // Try restarting all the cores to see if that fixes it
            attemptToRecoverCluster();
            checkClusterState();
        }
    }

    @AfterEach
    void after() throws Neo4jCluster.Neo4jTimeoutException, TimeoutException, IOException
    {
        // make sure that nothing is broken before the cluster is handed over to the next test
        try
        {
            checkClusterState();
        }
        catch ( Throwable e )
        {
            log.warn( () -> "After test: cluster not usable: " + e.getMessage() + "\nAttempting to recover..." );
            // Try restarting all the cores to see if that fixes it
            attemptToRecoverCluster();
            checkClusterState();
        }
    }

    private void attemptToRecoverCluster() throws Neo4jCluster.Neo4jTimeoutException, TimeoutException
    {
        try
        {
            cluster.killRandomServers( clusterSize );
        }
        catch ( Exception onKill )
        {
            log.warn( () -> "Servers could not be killed: " + onKill.getMessage() );
        }
        finally
        {
            cluster.startServers( cluster.getAllServers() );
            cluster.waitForLogMessageOnAll( cluster.getAllServers(), "Started.", Duration.ofMinutes( 2 ) );
        }
        cluster.waitForBoltOnAll( cluster.getAllServers(), Duration.ofMinutes( 2 ) );
    }

    /**
     * This test requires the first seed node form a new cluster after restarting its Akka System but then manages to re-join the other nodes who are in their
     * own cluster
     *
     * @param testSeed
     * @throws Exception
     */
    @ParameterizedTest()
    @ValueSource( booleans = {true, false} )
    @Order( 1 )
    void restartAkkaOnTheFirstSeedAndPause( boolean testSeed ) throws Exception
    {
        // given
        Set<Neo4jServer> testServer = pickTheTestServer( testSeed );
        Set<Neo4jServer> others = cluster.getAllServersExcept( testServer );
        int downingMessageCountBefore = countInDebugLogs( others, downingMessage );
        int akkaRestartMessageCountBefore = countInDebugLogs( testServer, akkaRestartMessage );
        int nodeJoiningItselfMessageCountBefore = countInDebugLogs( testServer, nodeJoiningItselfMessage );
        if ( testSeed )
        {
            assertThat( nodeJoiningItselfMessageCountBefore ).isGreaterThan( 0 );
        }

        // when
        cluster.pauseRandomServersExcept( 1, others );
        Instant deadline = Instant.now().plus( detectFailureRestartAkkaAndReformTimeout );

        waitUntil( deadline );
        assertThat( countInDebugLogs( others, downingMessage ) ).isEqualTo( downingMessageCountBefore + 1 );

        // Unpause the seed node and give it a second so that it will see that it has been booted out of the cluster - which will trigger an akka restart
        cluster.unpauseServers( testServer );
        waitForAkkaRestart( testServer, akkaRestartMessageCountBefore );

        // Now pause all the other nodes so that the seed cannot join them
        cluster.pauseRandomServersExcept( clusterSize - 1, testServer );
        try
        {
            // Give the seed long enough to form a cluster on its own
            waitUntil( Instant.now().plus( detectFailureRestartAkkaAndReformTimeout ) );
            if ( testSeed )
            {
                // This fails on 4.1 and below
                // We require that the node join itself in this situation
                assertThat( countInDebugLogs( testServer, nodeJoiningItselfMessage ) )
                        .as( "Node should join itself after restart" )
                        .isGreaterThan( nodeJoiningItselfMessageCountBefore );
            }
            else
            {
                assertThat( countInDebugLogs( testServer, nodeJoiningItselfMessage ) )
                        .as( "Node should not join itself after restart" )
                        .isEqualTo( nodeJoiningItselfMessageCountBefore );
            }
        }
        finally
        {
            // Now unpause the other servers and see what happens!
            cluster.unpauseServers( others );
            // Allow some time for the unpaused cores to figure out what is going on
            deadline = Instant.now().plus( Duration.ofSeconds( 10 ) );
        }

        // TODO: replace this wait with an assertion based on queries or logs
        waitUntil( deadline );
        checkClusterState();
    }

    private void waitUntil( Instant deadline ) throws InterruptedException
    {
        Duration timeToWait = Duration.between( Instant.now(), deadline );
        Thread.sleep( timeToWait.isNegative() ? 0 : timeToWait.toMillis() );
    }

    private int countInDebugLogs( Set<Neo4jServer> servers, String pattern )
    {
        log.info( () -> "Checking debug logs for " + pattern );
        String debugLogs = servers.stream().map( Neo4jServer::getDebugLog ).collect( Collectors.joining( "\n" ) );
        Matcher matcher = Pattern.compile( pattern ).matcher( debugLogs );
        int count = 0;
        while ( matcher.find() )
        {
            count++;
        }
        return count;
    }

    /**
     * This test requires the first seed node to be able to form a new cluster after restarting its Akka System.
     *
     * @param testSeed
     * @throws Exception
     */
    @ParameterizedTest()
    @ValueSource( booleans = {true, false} )
    @Order( 2 )
    void restartAkkaOnTheFirstSeedAndKill( boolean testSeed ) throws Exception
    {
        // given
        Set<Neo4jServer> testServer = pickTheTestServer( testSeed );
        Set<Neo4jServer> others = cluster.getAllServersExcept( testServer );
        int downingMessageCountBefore = countInDebugLogs( others, downingMessage );
        int akkaRestartMessageCountBefore = countInDebugLogs( testServer, akkaRestartMessage );
        int nodeJoiningItselfMessageCountBefore = countInDebugLogs( testServer, nodeJoiningItselfMessage );
        if ( testSeed )
        {
            assertThat( nodeJoiningItselfMessageCountBefore ).isGreaterThan( 0 );
        }

        // when
        cluster.pauseRandomServersExcept( 1, others );
        Instant deadline = Instant.now().plus( detectFailureRestartAkkaAndReformTimeout );

        waitUntil( deadline );
        assertThat( countInDebugLogs( others, downingMessage ) ).isEqualTo( downingMessageCountBefore + 1 );

        // Unpause the test node and give it a second so that it will see that it has been booted out of the cluster - which will trigger an akka restart
        cluster.unpauseServers( testServer );
        waitForAkkaRestart( testServer, akkaRestartMessageCountBefore );

        // Now kill all the other nodes so that the seed cannot join them
        cluster.killRandomServersExcept( clusterSize - 1, testServer );
        try
        {
            // Give the test node long enough to form a cluster on its own
            // case:
            //   testSeed: On 4.1 and below the seed cannot form a new cluster after a restart - it will just keep trying to join the others
            //   !testSeed: On 4.1 and below nobody can start a new cluster after a restart - everyone will just keep trying to join the everyone else
            waitUntil( Instant.now().plus( detectFailureRestartAkkaAndReformTimeout ) );
            if ( testSeed )
            {
                assertThat( countInDebugLogs( testServer, nodeJoiningItselfMessage ) )
                        .as( "Node should join itself after restart" )
                        .isGreaterThan( nodeJoiningItselfMessageCountBefore );
            }
            else
            {
                assertThat( countInDebugLogs( testServer, nodeJoiningItselfMessage ) )
                        .as( "Node should not join itself after restart" )
                        .isEqualTo( nodeJoiningItselfMessageCountBefore );
            }
        }
        finally
        {
            // Now start the other servers and see what happens!
            cluster.startServers( others );
        }

        // case:
        //   testSeed: fails on 4.1 and below because there is no Akka Cluster. A new cluster needs to be formed but the first seed won't create one.
        //   !testSeed: test fails on 4.1 and below. Akka cluster forms but cluster overview and show databases don't match on all cores
        checkClusterState();
    }

    private void waitForAkkaRestart( Set<Neo4jServer> testServer, int akkaRestartMessageCountBefore )
    {
        assertEventually(
                "Akka should restart",
                () -> countInDebugLogs( testServer, akkaRestartMessage ),
                equalityCondition( akkaRestartMessageCountBefore + 1 ),
                detectFailureRestartAkkaAndReformTimeout.toSeconds(), TimeUnit.SECONDS
        );
    }

    @ParameterizedTest()
    @ValueSource( booleans = {true, false} )
    @Order( 3 )
    void pauseOneAndRestartTheRest( boolean testSeed ) throws Exception
    {
        // given
        Set<Neo4jServer> testServer = pickTheTestServer( testSeed );
        Set<Neo4jServer> others = cluster.getAllServersExcept( testServer );

        //when
        cluster.pauseRandomServersExcept( 1, others );
        cluster.killRandomServersExcept( clusterSize - 1, testServer );
        Instant deadline = Instant.now().plus( Duration.ofSeconds( 10 ) );

        // case:
        //   testSeed: fails on 4.1 and below because no cluster will form
        //   !testSeed: A new 2-member cluster will form
        waitUntil( deadline );

        // startServers will start the container but block until the processes join the cluster, so we run that asynchronously
        // we wait for the log message that indicates the
        var started = CompletableFuture.supplyAsync( () -> cluster.startServers( others ) );
        assertEventually( "server containers started",
                          () -> others.stream().allMatch( Neo4jServer::isContainerRunning ),
                          TRUE, 3, TimeUnit.MINUTES );
        var othersLogs = others.stream().collect( Collectors.toMap( s -> s, Neo4jServer::getDebugLogPosition ) );
        deadline = Instant.now().plus( Duration.ofSeconds( 20 ) );

        // case:
        //   testSeed: the seed has a cluster (it was only paused) the other members should join it (they will be trying)
        //   !testSeed: The paused member should try and contact the 2-member cluster - but it's on a different Cluster incarnation, will it restart and join?
        waitUntil( deadline );
        assertEventuallyDoesNotThrow( "Started servers attempt to join",
                                      () -> othersLogs.forEach( ( server, debugLogPosition ) -> assertThat( server.getDebugLogFrom( debugLogPosition ) )
                                              .contains( "Joining seed nodes" ) ),
                                      2, TimeUnit.MINUTES );

        cluster.unpauseServers( testServer );
        deadline = Instant.now().plus( Duration.ofSeconds( 10 ) );

        // after unpausing we need a few seconds for the core to notice that it has been unpaused
        waitUntil( deadline );
        started.join();
        // case:
        //   testSeed: test passes
        //   !testSeed: fails on 4.1 and below because - akka cluster is partitioned into a 2-member cluster and a 1-member cluster
        checkClusterState();
    }

    private Set<Neo4jServer> pickTheTestServer( boolean testSeed ) throws ExecutionException, InterruptedException, TimeoutException
    {
        Set<Neo4jServer> seed = Set.of( getSeed() );
        Set<Neo4jServer> notTheSeed = cluster.getAllServersExcept( seed );

        return testSeed ? seed : notTheSeed.stream().limit( 1 ).collect( Collectors.toSet() );
    }

    private Neo4jServer getSeed() throws ExecutionException, InterruptedException, TimeoutException
    {
        URI firstAkkaSeed = getFirstAkkaSeed( clusterChecker );
        return cluster.getAllServers().stream().filter( s -> s.getDirectBoltUri().equals( firstAkkaSeed ) ).findFirst().get();
    }

    private void checkClusterState()
    {
        assertEventuallyDoesNotThrow(
                "All servers should show the same state",
                () ->
                {
                    var akkaState = AkkaState.verifyAllServersShowSameAkkaState( clusterChecker );
                    assertThat( akkaState.getMembers() ).hasSize( clusterSize );
                    assertThat( akkaState.getUnreachable() ).isEmpty();
                    clusterChecker.verifyClusterStateMatchesOnAllServers();
                },
                6, TimeUnit.MINUTES
        );
        assertEventuallyDoesNotThrow( "metrics should look ok", () -> {
            AkkaState.checkMetrics( cluster.getAllServersOfType( Neo4jServer.Type.CORE_SERVER ), driverFactory, 2 );
        }, 2, TimeUnit.MINUTES, 15, TimeUnit.SECONDS );

    }

    private static URI getFirstAkkaSeed( ClusterChecker checker ) throws ExecutionException, InterruptedException, TimeoutException
    {
        var results = checker.runOnAllServers(
                "CALL dbms.listConfig('initial_discovery_members') YIELD value WITH value AS initial_members " +
                "CALL dbms.listConfig('discovery_advertised_address') YIELD value WITH value AS advertised_address, initial_members " +
                "RETURN initial_members, advertised_address"
        );

        final Set<String> firstDiscoveryMembers = new HashSet<>();
        List<URI> firstSeeds = results.entrySet()
                                      .stream()
                                      .filter( entry ->
                                               {
                                                   assertThat( entry.getValue() ).hasSize( 1 );

                                                   // If the cypher response was `true`
                                                   Record result = entry.getValue().get( 0 );
                                                   List<String> initialMembers =
                                                           Arrays.stream( result.get( "initial_members" ).asString().split( "," ) )
                                                                 .map( String::trim )
                                                                 .collect( Collectors.toList() );

                                                   // When LIST is used these are sorted alphabetically by neo4j
                                                   String firstDiscoveryMember = initialMembers.stream().sorted().findFirst().get();
                                                   firstDiscoveryMembers.add( firstDiscoveryMember );
                                                   assertThat( firstDiscoveryMembers )
                                                           .as( "Cores must agree on who the first discovery member is" )
                                                           .hasSize( 1 );

                                                   String advertisedAddress = result.get( "advertised_address" ).asString();
                                                   return advertisedAddress.equalsIgnoreCase( firstDiscoveryMember );
                                               } )
                                      .map( Map.Entry::getKey )
                                      .collect( Collectors.toList() );

        assertThat( firstSeeds ).hasSize( 1 );
        return firstSeeds.get( 0 );
    }
}
