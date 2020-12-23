/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readonly;

import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import com.neo4j.test.driver.DriverTestHelper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.test.extension.Inject;

import static com.neo4j.configuration.CausalClusteringSettings.cluster_topology_refresh;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DriverExtension
@ClusterExtension
public class ReadOnlyDriverIT
{
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private DriverFactory driverFactory;

    @Test
    void shouldFailToWriteOnReadOnlyMember() throws TimeoutException, ExecutionException, InterruptedException, IOException
    {
        //given
        final var config = getClusterConfig( Set.of(), true );
        var cluster = clusterFactory.createCluster( config );
        cluster.start();
        cluster.awaitLeader();
        final var driver = driverFactory.graphDatabaseDriver( cluster );

        //when/then
        assertThrows( DatabaseException.class, () -> DriverTestHelper.writeData( driver ) );
    }

    private ClusterConfig getClusterConfig( Set<String> readOnlyDatabases, boolean globalReadOnly )
    {
        var config = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( cluster_topology_refresh, "5s" );

        var dbsStr = String.join( ",", readOnlyDatabases );

        return config.withSharedCoreParam( GraphDatabaseSettings.read_only_databases, dbsStr )
                     .withSharedCoreParam( GraphDatabaseSettings.read_only_database_default, String.valueOf( globalReadOnly ) );
    }
}
