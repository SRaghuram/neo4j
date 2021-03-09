/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.routing;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ExtendWith( MockedRoutingContextExtension.class )
@ClusterExtension
@ResourceLock( Resources.SYSTEM_OUT )
class ClusterRoutingFailureIT
{
    @Inject
    private static ClusterFactory clusterFactory;

    private Cluster cluster;
    private Driver driver;

    @Test
    void testWriteOnFollowerWithNoLeader() throws ExecutionException, InterruptedException, TimeoutException
    {

        try
        {
            ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                                                       .withNumberOfCoreMembers( 2 )
                                                       .withSharedCoreParam( GraphDatabaseSettings.routing_enabled, "true" );

            cluster = clusterFactory.createCluster( clusterConfig );
            cluster.start();
            cluster.awaitLeader( "neo4j" ).shutdown();
            driver = driver( cluster.randomCoreMember( true ).get().directURI() );

            // When we need routing, but there is no leader we should get NotALeader because that triggers routing table refresh in the driver
            assertThatExceptionOfType( ClientException.class )
                    .isThrownBy( () -> run( driver, "neo4j", AccessMode.WRITE, tx -> tx.run( "CREATE ()" ) ) )
                    .satisfies( err -> assertThat( err.code() ).isEqualTo( Status.Cluster.NotALeader.code().serialize() ) );
        }
        finally
        {
            if ( driver != null )
            {
                driver.close();
            }
            if ( cluster != null )
            {
                cluster.shutdown();
            }
        }
    }

    private static Driver driver( String uri )
    {
        return GraphDatabase.driver(
                uri,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .withMaxConnectionPoolSize( 3 )
                                       .build() );
    }

    private static <T> T run( Driver driver, String databaseName, AccessMode mode, Function<Session,T> workload )
    {
        try ( var session = driver.session( SessionConfig.builder()
                                                         .withDatabase( databaseName )
                                                         .withDefaultAccessMode( mode )
                                                         .build() ) )
        {
            return workload.apply( session );
        }
    }
}
