/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.harness;

import com.neo4j.harness.CausalClusterInProcessBuilder;
import com.neo4j.harness.internal.CommercialInProcessServerBuilder;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Path;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;

public class CausalClusterInProcessRunnerIT
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
                        .withBuilder( CommercialInProcessServerBuilder::new )
                        .withCores( 3 )
                        .withReplicas( 3 )
                        .withLogger( NullLogProvider.getInstance() )
                        .atPath( clusterPath )
                        .withOptionalPortsStrategy( portPickingStrategy )
                        .build();

        cluster.boot();
        cluster.shutdown();
    }
}
