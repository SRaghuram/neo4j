/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness;

import com.neo4j.configuration.SecuritySettings;
import com.neo4j.harness.internal.CausalClusterInProcessBuilder;
import com.neo4j.harness.internal.EnterpriseInProcessNeo4jBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

@TestDirectoryExtension
class CausalClusterInProcessRunnerIT
{
    @Inject
    private TestDirectory testDirectory;

    private CausalClusterInProcessBuilder.CausalCluster cluster;

    @AfterEach
    void afterEach()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    void shouldBootAndShutdownCluster()
    {
        var clusterPath = testDirectory.homeDir().toPath();
        var portPickingStrategy = new PortAuthorityPortPickingStrategy();

        cluster = CausalClusterInProcessBuilder.init()
                .withBuilder( EnterpriseInProcessNeo4jBuilder::new )
                .withCores( 3 )
                .withReplicas( 3 )
                .withLogger( NullLogProvider.getInstance() )
                .atPath( clusterPath )
                .withOptionalPortsStrategy( portPickingStrategy )
                .build();

        cluster.boot();
        cluster.shutdown();
    }

    @Test
    void shouldBootAndShutdownSecureCluster()
    {
        var clusterPath = testDirectory.homeDir().toPath();
        var portPickingStrategy = new PortAuthorityPortPickingStrategy();

        cluster = CausalClusterInProcessBuilder.init()
                .withBuilder( EnterpriseInProcessNeo4jBuilder::new )
                .withCores( 3 )
                .withReplicas( 3 )
                .withLogger( NullLogProvider.getInstance() )
                .atPath( clusterPath )
                .withConfig( GraphDatabaseSettings.auth_enabled, true )
                .withConfig( SecuritySettings.authentication_providers, List.of( SecuritySettings.NATIVE_REALM_NAME ) )
                .withConfig( SecuritySettings.authorization_providers, List.of( SecuritySettings.NATIVE_REALM_NAME ) )
                .withOptionalPortsStrategy( portPickingStrategy )
                .build();

        cluster.boot();
        cluster.shutdown();
    }
}
