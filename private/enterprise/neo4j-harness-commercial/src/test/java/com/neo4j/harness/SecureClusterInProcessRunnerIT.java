/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness;

import com.neo4j.harness.internal.CausalClusterInProcessBuilder;
import com.neo4j.harness.internal.CommercialInProcessNeo4jBuilder;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Path;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.PortAuthorityPortPickingStrategy;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;

public class SecureClusterInProcessRunnerIT
{
    @ClassRule
    public static final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldBootAndShutdownCluster() throws Exception
    {
        Path clusterPath = testDirectory.directory().toPath();
        CausalClusterInProcessBuilder.PortPickingStrategy portPickingStrategy = new PortAuthorityPortPickingStrategy();

        CausalClusterInProcessBuilder.CausalCluster cluster =
                CausalClusterInProcessBuilder.init()
                        .withBuilder( CommercialInProcessNeo4jBuilder::new )
                        .withCores( 3 )
                        .withReplicas( 3 )
                        .withLogger( NullLogProvider.getInstance() )
                        .atPath( clusterPath )
                        .withConfig( GraphDatabaseSettings.auth_enabled.name(), "true" )
                        .withConfig( SecuritySettings.auth_provider.name(), SecuritySettings.SYSTEM_GRAPH_REALM_NAME )
                        .withOptionalPortsStrategy( portPickingStrategy )
                        .build();

        cluster.boot();
        cluster.shutdown();
    }
}
