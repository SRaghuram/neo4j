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
import org.junit.jupiter.api.BeforeEach;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

import static org.neo4j.test.assertion.Assert.assertEventuallyDoesNotThrow;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@NeedsCausalCluster( numberOfCoreServers = 5 )
@DriverExtension
@ExtendWith( DumpDockerLogs.class )
@TestMethodOrder( MethodOrderer.OrderAnnotation.class )
public class BigClusterAkkaTest
{
    private static final AuthToken authToken = AuthTokens.basic( "neo4j", "password" );
    private static final DriverFactory.InstanceConfig driverConfig = DriverFactory.instanceConfig().withAuthToken( authToken );

    @Inject
    private DriverFactory driverFactory;

    @CausalCluster
    private static Neo4jCluster cluster;

    private final Logger log = LoggerFactory.getLogger( this.getClass() );

    private Driver driver;
    private final MetricsReader metricsReader = new MetricsReader();
    private int databaseCount = 2;

    @CoreModifier
    private static Neo4jContainer<?> configure( Neo4jContainer<?> input ) throws IOException
    {
        return DeveloperWorkflow.configureNeo4jContainerIfNecessary( input )
                                .withNeo4jConfig( "metrics.filter", "*" )
                                .withNeo4jConfig( "causal_clustering.minimum_core_cluster_size_at_formation", "5" )
                                .withNeo4jConfig( "causal_clustering.minimum_core_cluster_size_at_runtime", "5" );
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
        // let's try to add a few extra databases
        databaseCount = ensureDatabaseCount( 5, driver );
    }

    @AfterEach
    void after()
    {
        // make sure that nothing is broken before the cluster is handed over to the next test
        driver.verifyConnectivity();

        // make sure every container is running
        cluster.startServers( cluster.getAllServers().stream().filter( server -> !server.isContainerRunning() ).collect( Collectors.toSet() ) );
        checkMetrics();
    }

    @Test
    @Order( 1 )
    void stopOneServerTest() throws Exception
    {
        // when
        cluster.stopRandomServers( 2 );
        Thread.sleep( 100 );

        // then
        driver.verifyConnectivity();
        checkMetrics();
    }

    @Test
    @Order( 2 )
    void killOneServerTest() throws Exception
    {
        // when
        cluster.killRandomServers( 2 );
        Thread.sleep( 100 );

        // then
        driver.verifyConnectivity();
        checkMetrics();
    }

    private int ensureDatabaseCount( int expectedCount, Driver driver )
    {
        try ( var session = driver.session( SessionConfig.forDatabase( "system" ) ) )
        {
            var actualCount = (int) session.run( "SHOW DATABASES YIELD name" ).stream().map( r -> r.get( "name" ) ).distinct().count();
            while ( actualCount < expectedCount )
            {
                session.run( "CREATE DATABASE $newDb WAIT", Map.of( "newDb", "testDB" + actualCount ) ).consume();
                actualCount++;
            }
            return actualCount;
        }
    }

    private void checkMetrics()
    {
        assertEventuallyDoesNotThrow( "metrics should look ok", () ->
        {
            AkkaState.checkMetrics( cluster.getAllServersOfType( Neo4jServer.Type.CORE_SERVER ),
                                    uri -> driverFactory.graphDatabaseDriver( uri, driverConfig ),
                                    databaseCount );
        }, 2, TimeUnit.MINUTES, 15, TimeUnit.SECONDS );
    }
}
