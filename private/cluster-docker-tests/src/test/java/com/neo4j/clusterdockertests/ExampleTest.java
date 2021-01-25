/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.CoreModifier;
import org.neo4j.junit.jupiter.causal_cluster.NeedsCausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jServer;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@NeedsCausalCluster
@DriverExtension
@ExtendWith( DumpDockerLogs.class )
public class ExampleTest
{
    private static final AuthToken authToken = AuthTokens.basic( "neo4j", "password" );

    @Inject
    private DriverFactory driverFactory;

    @CausalCluster
    private static Neo4jCluster cluster;

    private final Logger log = LoggerFactory.getLogger( this.getClass() );

    private Driver driver;

    @CoreModifier
    private static Neo4jContainer<?> configure( Neo4jContainer<?> input ) throws IOException
    {
        return DeveloperWorkflow.configureNeo4jContainerIfNecessary( input );
    }

    @BeforeAll
    void setUp()
    {
        driverFactory.setAuthToken( authToken );
    }

    @AfterAll
    void tearDown()
    {
        this.driver.close();
    }

    @BeforeEach
    void before() throws IOException
    {
        driver = driverFactory.graphDatabaseDriver( cluster.getURIs() );
        // make sure that cluster is ready to go before we start
        driver.verifyConnectivity();
    }

    @AfterEach
    void after()
    {
        // make sure that nothing is broken before the cluster is handed over to the next test
        driver.verifyConnectivity();
    }

    @Test
    void stopStartOneServerTest() throws Neo4jCluster.Neo4jTimeoutException, InterruptedException
    {
        // when
        Set<Neo4jServer> stopped = cluster.stopRandomServers( 1 );

        Thread.sleep( 100 );

        // when
        Set<Neo4jServer> started = cluster.startServers( stopped );
        cluster.waitForBoltOnAll( started, Duration.ofMinutes( 1 ) );

        // then
        assertThat( started ).containsExactlyInAnyOrderElementsOf( stopped );
        driver.verifyConnectivity();
    }

    @Test
    void killStartOneServerTest() throws Neo4jCluster.Neo4jTimeoutException, InterruptedException
    {
        // when
        Set<Neo4jServer> stopped = cluster.killRandomServers( 1 );

        Thread.sleep( 100 );

        // when
        Set<Neo4jServer> started = cluster.startServers( stopped );
        cluster.waitForBoltOnAll( started, Duration.ofMinutes( 1 ) );

        // then
        assertThat( started ).containsExactlyInAnyOrderElementsOf( stopped );
        driver.verifyConnectivity();
    }
}
