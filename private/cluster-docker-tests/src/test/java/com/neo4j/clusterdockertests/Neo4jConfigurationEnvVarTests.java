/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;

import java.io.IOException;
import java.util.Set;
import java.util.function.Consumer;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.CoreModifier;
import org.neo4j.junit.jupiter.causal_cluster.NeedsCausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jServer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test checks that the expectations we have about the neo4j cluster based on magic environment variables are met. These Environment Variables are
 * generally used in TeamCity
 */
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@NeedsCausalCluster
@ExtendWith( DumpDockerLogs.class )
public class Neo4jConfigurationEnvVarTests
{
    private static final AuthToken authToken = AuthTokens.basic( "neo4j", "password" );

    @CausalCluster
    private static Neo4jCluster cluster;

    private Driver driver;

    @CoreModifier
    private static Neo4jContainer<?> configure( Neo4jContainer<?> input ) throws IOException
    {
        return DeveloperWorkflow.configureNeo4jContainerIfNecessary( input );
    }

    private final Logger log = LoggerFactory.getLogger( this.getClass() );

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

    @Test
    public void CheckNeo4jVersionIfNecessary()
    {
        // when asked the cluster should always give us a version
        String runningNeo4jVersion = getNeo4jVersion();
        log.info( () -> "Neo4j Version: " + runningNeo4jVersion );

        // if a version requirement/expectation has been set as an env var then we must honour it
        String requiredNeo4jVersion = System.getenv().getOrDefault( "NEO4JVERSION", "" ).trim();
        if ( !requiredNeo4jVersion.isEmpty() )
        {
            assertThat( runningNeo4jVersion ).isEqualTo( requiredNeo4jVersion );
        }
    }

    @Test
    public void CheckExtensionScript()
    {
        // given
        String useDeveloperWorkflowEnvVar = System.getenv().getOrDefault( "CLUSTER_DOCKER_TESTS_LOAD_RUNTIME_CLASSES", "true" ).trim();
        assertThat( Set.of( "true", "false" ) ).as( "Supported values" ).contains( useDeveloperWorkflowEnvVar );

        // if the developer workflow is enabled we expect to see the log message from its setup script.
        // if it is disabled we expect _not_ to see that log message.
        String expectedMessage = "dev extension script completed.";
        Consumer<Neo4jServer> check = Boolean.parseBoolean( useDeveloperWorkflowEnvVar ) ?
                                      s -> assertThat( s.getContainerLogs() ).contains( expectedMessage ) :
                                      s -> assertThat( s.getContainerLogs() ).doesNotContain( expectedMessage );

        cluster.getAllServers().forEach( check );
    }

    private String getNeo4jVersion()
    {
        try ( Driver driver = GraphDatabase.driver(
                cluster.getURI(),
                authToken,
                Config.defaultConfig() );
              Session session = driver.session();
        )
        {
            String version = session.readTransaction(
                    tx -> tx.run( "call dbms.components() yield name, versions, edition " +
                                  "UNWIND versions as version return name, version, edition "
                    ).single().get( "version" ).asString()
            );

            return version;
        }
    }
}
