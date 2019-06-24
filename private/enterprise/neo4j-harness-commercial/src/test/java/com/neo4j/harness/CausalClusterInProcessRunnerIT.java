/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness;

import com.neo4j.harness.internal.CausalClusterInProcessBuilder;
import com.neo4j.harness.internal.CommercialInProcessNeo4jBuilder;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.SettingValueParsers.TRUE;

@ExtendWith( TestDirectoryExtension.class )
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
        var clusterPath = testDirectory.directory().toPath();
        var portPickingStrategy = new PortAuthorityPortPickingStrategy();

        cluster = CausalClusterInProcessBuilder.init()
                .withBuilder( CommercialInProcessNeo4jBuilder::new )
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
        var clusterPath = testDirectory.directory().toPath();
        var portPickingStrategy = new PortAuthorityPortPickingStrategy();

        cluster = CausalClusterInProcessBuilder.init()
                .withBuilder( CommercialInProcessNeo4jBuilder::new )
                .withCores( 3 )
                .withReplicas( 3 )
                .withLogger( NullLogProvider.getInstance() )
                .atPath( clusterPath )
                .withConfig( GraphDatabaseSettings.auth_enabled.name(), TRUE )
                .withConfig( SecuritySettings.authentication_providers.name(), SecuritySettings.NATIVE_REALM_NAME )
                .withConfig( SecuritySettings.authorization_providers.name(), SecuritySettings.NATIVE_REALM_NAME )
                .withOptionalPortsStrategy( portPickingStrategy )
                .build();

        cluster.boot();
        cluster.shutdown();
    }
}
