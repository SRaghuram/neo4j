/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readonly;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import com.neo4j.test.driver.DriverTestHelper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.logging.Level;
import org.neo4j.test.extension.Inject;

import static com.neo4j.configuration.CausalClusteringSettings.cluster_topology_refresh;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@DriverExtension
@ClusterExtension
public class ReadOnlyDriverIT
{
    private static final String driverTransactionRetryMessage = "Transaction failed and will be retried";
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private DriverFactory driverFactory;
    private DriverFactory.InstanceConfig driverConfig = DriverFactory.instanceConfig()
                                                                     .withLogLevel( Level.DEBUG )
                                                                     .withAdditionalConfig(
                                                                             c -> c.withMaxTransactionRetryTime( 3, TimeUnit.SECONDS )
                                                                     );

    @Test
    void shouldFailToWriteOnReadOnlyMember() throws TimeoutException, ExecutionException, InterruptedException, IOException
    {
        // given
        final var config = getClusterConfig( Set.of(), true );
        var cluster = clusterFactory.createCluster( config );
        cluster.start();
        cluster.awaitLeader();

        // when/then
        final var driver = driverFactory.graphDatabaseDriver( cluster, driverConfig );
        // try-with-resources is used for the driver because we need to close the driver to be sure that the logs are flushed for the subsequent asserts
        try ( driver )
        {
            assertThrows( DatabaseException.class, () -> DriverTestHelper.writeData( driver ) );
        }
        var driverLog = Files.readString( driverFactory.getLogFile( driver ) );
        assertThat( driverLog ).doesNotContainIgnoringCase( "retried" )
                               .doesNotContainIgnoringCase( "retrying" )
                               .doesNotContainIgnoringCase( driverTransactionRetryMessage );

        // We just asserted that the driver doesn't retry based on some magic strings.
        // To avoid regression let's just confirm that the driver does log those magic strings when it does a retry

        // given
        // Shut down all primary members except for one, this will create a routing table that doesn't contain any writers.
        // in this situation the driver is expected to retry until the driver-side-configured MaxTransactionRetryTime is exceeded.
        Cluster.shutdownMembers( cluster.primaryMembers().stream().skip(1).toArray( ClusterMember[]::new ) );
        CoreClusterMember core = cluster.randomCoreMember( true ).get();
        var leaderService = core.resolveDependency( DEFAULT_DATABASE_NAME, LeaderService.class );

        // when
        assertEventually( "there should be no leader",
                          () -> leaderService.getLeaderId( core.databaseId( DEFAULT_DATABASE_NAME ) ),
                          Optional::isEmpty,
                          core.settingValue( CausalClusteringSettings.leader_failure_detection_window ).getMax().toSeconds() + 10 , TimeUnit.SECONDS
        );

        // then
        // try-with-resources is used for the driver because we need to close the driver to be sure that the logs are flushed for the subsequent asserts
        final var driver2 = driverFactory.graphDatabaseDriver( cluster, driverConfig );
        try ( driver2 )
        {
            assertThrows( SessionExpiredException.class, () -> DriverTestHelper.writeData( driver2 ) );
        }
        driverLog = Files.readString( driverFactory.getLogFile( driver2 ) );
        assertThat( driverLog ).contains( driverTransactionRetryMessage );
    }

    private ClusterConfig getClusterConfig( Set<String> readOnlyDatabases, boolean defaultToReadOnly )
    {
        var config = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedPrimaryParam( cluster_topology_refresh, "5s" );

        var dbsStr = String.join( ",", readOnlyDatabases );

        return config.withSharedPrimaryParam( GraphDatabaseSettings.read_only_databases, dbsStr )
                     .withSharedPrimaryParam( GraphDatabaseSettings.read_only_database_default, String.valueOf( defaultToReadOnly ) )
                ;
    }
}
