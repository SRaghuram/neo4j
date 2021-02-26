/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import com.neo4j.test.driver.ClusterChecker;
import com.neo4j.test.driver.ClusterCheckerExtension;
import com.neo4j.test.driver.ClusterCheckerFactory;
import com.neo4j.test.driver.DriverFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.CoreModifier;
import org.neo4j.junit.jupiter.causal_cluster.NeedsCausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jServer;
import org.neo4j.test.extension.Inject;

import static com.neo4j.clusterdockertests.MetricsReader.isEqualTo;
import static com.neo4j.clusterdockertests.MetricsReader.replicatedDataMetric;
import static com.neo4j.configuration.MetricsSettings.metrics_filter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventuallyDoesNotThrow;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@NeedsCausalCluster
@ClusterCheckerExtension
@ExtendWith( DumpDockerLogs.class )
public class ExampleTest
{
    private static final AuthToken authToken = AuthTokens.basic( "neo4j", "password" );
    private static final DriverFactory.InstanceConfig driverConfig = DriverFactory.instanceConfig().withAuthToken( authToken );

    @Inject
    private DriverFactory driverFactory;

    @Inject
    private ClusterCheckerFactory clusterCheckerFactory;

    @CausalCluster
    private static Neo4jCluster cluster;

    private final Logger log = LoggerFactory.getLogger( this.getClass() );
    private final MetricsReader metricsReader = new MetricsReader();

    private Driver driver;
    private ClusterChecker clusterChecker;
    private int expectedNumberOfDatabases = 2;

    @CoreModifier
    private static Neo4jContainer<?> configure( Neo4jContainer<?> input ) throws IOException
    {
        return DeveloperWorkflow.configureNeo4jContainerIfNecessary( input )
                                .withNeo4jConfig( metrics_filter.name(), "*" );
    }

    @AfterAll
    void tearDown()
    {
        this.driver.close();
    }

    @BeforeEach
    void before() throws IOException
    {
        driver = driverFactory.graphDatabaseDriver( cluster.getURIs(), driverConfig );
        // make sure that cluster is ready to go before we start
        driver.verifyConnectivity();
        Set<URI> boltURIs = cluster.getAllServersOfType( Neo4jServer.Type.CORE_SERVER )
                                  .stream()
                                  .map( Neo4jServer::getDirectBoltUri )
                                  .collect( Collectors.toSet() );

        clusterChecker = clusterCheckerFactory.clusterChecker( boltURIs, driverConfig );
    }

    @AfterEach
    void after()
    {
        // make sure that nothing is broken before the cluster is handed over to the next test
        driver.verifyConnectivity();
    }

    @Test
    void stopStartOneServerTest() throws Exception
    {
        // when
        Set<Neo4jServer> stopped = cluster.stopRandomServers( 1 );

        try ( var session = driver.session( SessionConfig.forDatabase( "system" ) ) )
        {
            session.writeTransaction( tx -> tx.run( "CREATE DATABASE `stopStartOneServerTest` WAIT" ).consume() );
            expectedNumberOfDatabases += 1;
        }

        // when
        Set<Neo4jServer> started = cluster.startServers( stopped );
        cluster.waitForBoltOnAll( started, Duration.ofMinutes( 1 ) );

        // then
        assertThat( started ).containsExactlyInAnyOrderElementsOf( stopped );
        driver.verifyConnectivity();
        assertEventuallyDoesNotThrow( "Check Leaderships", this::checkLeaderships, 2, TimeUnit.MINUTES );
        assertEventuallyDoesNotThrow( "Check Metrics", this::checkMetrics, 2, TimeUnit.MINUTES );
    }

    @Test
    void killStartOneServerTest() throws Exception
    {
        // when
        Set<Neo4jServer> stopped = cluster.killRandomServers( 1 );

        try ( var session = driver.session( SessionConfig.forDatabase( "system" ) ) )
        {
            session.writeTransaction( tx -> tx.run( "CREATE DATABASE `killStartOneServerTest` WAIT" ).consume() );
            expectedNumberOfDatabases += 1;
        }

        // when
        Set<Neo4jServer> started = cluster.startServers( stopped );
        cluster.waitForBoltOnAll( started, Duration.ofMinutes( 1 ) );

        // then
        assertThat( started ).containsExactlyInAnyOrderElementsOf( stopped );
        driver.verifyConnectivity();
        assertEventuallyDoesNotThrow( "Check Leaderships", this::checkLeaderships, 2, TimeUnit.MINUTES );
        assertEventuallyDoesNotThrow( "Check Metrics", this::checkMetrics, 2, TimeUnit.MINUTES );
    }

    private void checkMetrics() throws IOException
    {
        var expectations = new MetricsReader.MetricExpectations()
                .add( replicatedDataMetric( "per_db_leader_name.visible" ), isEqualTo( expectedNumberOfDatabases ) );

        for ( var server : cluster.getAllServers() )
        {
            try ( var driver = driverFactory.graphDatabaseDriver( server.getDirectBoltUri(), driverConfig ) )
            {
                metricsReader.checkExpectations( driver, expectations );
            }
        }
    }

    private void checkLeaderships() throws InterruptedException, ExecutionException, TimeoutException
    {
        assertThat( clusterChecker.areLeadershipsWellBalanced( expectedNumberOfDatabases ) )
                .as( "leaderships should be well balanced" )
                .isTrue();
    }
}
