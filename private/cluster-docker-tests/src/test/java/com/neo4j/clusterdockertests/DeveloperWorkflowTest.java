/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Neo4jContainer;

import java.io.IOException;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.CoreModifier;
import org.neo4j.junit.jupiter.causal_cluster.NeedsCausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test _always_ loads the current classpath into the docker image in order to tests that this mode works and doesn't regress
 */
@NeedsCausalCluster
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@ExtendWith( DumpDockerLogs.class )
public class DeveloperWorkflowTest
{
    private static final AuthToken authToken = AuthTokens.basic( "neo4j", "password" );

    @CausalCluster
    private static Neo4jCluster cluster;

    private Driver driver;

    @CoreModifier
    private static Neo4jContainer<?> configure( Neo4jContainer<?> input ) throws IOException
    {
        return DeveloperWorkflow.configureNeo4jContainerIfNecessary( input, true );
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

    @Test
    public void print()
    {
        cluster.getAllServers().forEach( s -> assertThat( s.getContainerLogs() ).contains( "dev extension script completed." ) );
        driver.verifyConnectivity();
    }
}
