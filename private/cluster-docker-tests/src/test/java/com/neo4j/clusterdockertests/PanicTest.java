/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.EnterpriseEditionSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.CoreModifier;
import org.neo4j.junit.jupiter.causal_cluster.NeedsCausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jServer;

import static org.neo4j.test.assertion.Assert.assertEventually;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@NeedsCausalCluster
@ExtendWith( DumpDockerLogs.class )
public class PanicTest
{
    private static final AuthToken authToken = AuthTokens.basic( "neo4j", "password" );

    @CausalCluster
    private static Neo4jCluster cluster;

    private final Logger log = LoggerFactory.getLogger( this.getClass() );

    @CoreModifier
    private static Neo4jContainer<?> configure( Neo4jContainer<?> input ) throws IOException
    {
        return DeveloperWorkflow.configureNeo4jContainerIfNecessary( input )
                                .withNeo4jConfig( CausalClusteringInternalSettings.akka_actor_system_max_restart_attempts.name(), "0" )
                                .withNeo4jConfig( EnterpriseEditionSettings.shutdown_on_dbms_panic.name(), "true" );
    }

    @Test
    void panicDbms()
    {
        // if we pause a majority of servers the remaining server will restart
        Set<Neo4jServer> paused = cluster.pauseRandomServers( 2 );

        var runningSever = cluster.getAllServersExcept( paused ).stream().findFirst().get();

        assertEventually( runningSever::getContainerLogs,
                          logs -> logs.contains( "The Neo4j Database Management System has panicked. The process will now shut down." ),
                          3, TimeUnit.MINUTES );

        assertEventually( runningSever::isContainerRunning,
                          isRunning -> isRunning == false,
                          3, TimeUnit.MINUTES );
    }
}
