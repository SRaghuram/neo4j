/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.CoreModifier;
import org.neo4j.junit.jupiter.causal_cluster.NeedsCausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.neo4j.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This type of testing belongs better in the stresstests package. However we do not have a way to simulate GC/VM pauses in those tests. So this test, but only
 * this test is here.
 *
 * Test method ordering is required because server.getDebugLog() returns the debug logs for the lifetime of the whole test class. This is fine because the first
 * two tests assert that the logs do not contain "UNREACHABLE" and the final test asserts that they don't. If it became necessary to add another test after then
 * we'd have to improve how we handle that whole debug log situation.
 */
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@NeedsCausalCluster
@ExtendWith( DumpDockerLogs.class )
@TestMethodOrder( MethodOrderer.OrderAnnotation.class )
public class HighGCStressTest
{
    private static final AuthToken authToken = AuthTokens.basic( "neo4j", "password" );
    private static final int ACCEPTABLE_PAUSE = 7;

    @CausalCluster
    private static Neo4jCluster cluster;

    private final Logger log = LoggerFactory.getLogger( this.getClass() );

    private Driver driver;

    @CoreModifier
    private static Neo4jContainer<?> configure( Neo4jContainer<?> input ) throws IOException
    {
        return DeveloperWorkflow.configureNeo4jContainerIfNecessary( input )
                                .withNeo4jConfig( CausalClusteringInternalSettings.akka_failure_detector_acceptable_heartbeat_pause.name(),
                                                  ACCEPTABLE_PAUSE + "s" )
                                .withNeo4jConfig( CausalClusteringSettings.middleware_logging_level.name(),
                                                  Level.INFO.toString() );
    }

    @BeforeAll
    void setUp()
    {
        this.driver = GraphDatabase.driver( cluster.getURI(), authToken );
    }

    @AfterAll
    void tearDown()
    {
        this.driver.close();
    }

    @Order( 1 )
    @Test
    void repeatedShortPausesAcrossManyServersShouldNotCauseUnreachability() throws InterruptedException
    {
        // given
        var pauses = IntStream.generate( () -> ThreadLocalRandom.current().nextInt( ACCEPTABLE_PAUSE ) );

        // when
        var pauseList = pauses.boxed().limit( 10 ).collect( Collectors.toList() );
        for ( var pause : pauseList )
        {
            var paused = cluster.pauseRandomServers( 1 );
            Thread.sleep( pause * 1000 );
            cluster.unpauseServers( paused );
            Thread.sleep( ACCEPTABLE_PAUSE );
        }

        // then
        for ( var server : cluster.getAllServers() )
        {
            assertThat( server.getDebugLog() ).doesNotContain( "UNREACHABLE" );
        }

        this.driver.verifyConnectivity();
    }

    @Order( 2 )
    @Test
    void repeatedShortPausesOnSingleServerShouldNotCauseUnreachability() throws InterruptedException
    {
        // given
        var pauses = IntStream.generate( () -> ThreadLocalRandom.current().nextInt( ACCEPTABLE_PAUSE ) );

        // when
        var pauseList = pauses.boxed().limit( 10 ).collect( Collectors.toList() );

        var toPause = cluster.getAllServers().stream().findFirst().get();
        var others = cluster.getAllServersExcept( toPause );

        for ( var pause : pauseList )
        {
            //Pauses single server over and over
            var paused = cluster.pauseRandomServersExcept( 1, others );
            Thread.sleep( pause * 1000 );
            cluster.unpauseServers( paused );
            Thread.sleep( ACCEPTABLE_PAUSE );
        }

        // then
        for ( var server : cluster.getAllServers() )
        {
            assertThat( server.getDebugLog() ).doesNotContain( "UNREACHABLE" );
        }

        this.driver.verifyConnectivity();
    }

    @Order( 3 )
    @Test
    void longPauseShouldCauseUnreachable() throws InterruptedException
    {
        // given
        var longPause = ACCEPTABLE_PAUSE * 3;

        // when
        var paused = cluster.pauseRandomServers( 1 );
        Thread.sleep( longPause * 1000 );
        cluster.unpauseServers( paused );
        Thread.sleep( ACCEPTABLE_PAUSE );

        // then
        for ( var server : cluster.getAllServersExcept( paused ) )
        {
            assertThat( server.getDebugLog() ).contains( "UNREACHABLE" );
        }

        this.driver.verifyConnectivity();
    }
}
