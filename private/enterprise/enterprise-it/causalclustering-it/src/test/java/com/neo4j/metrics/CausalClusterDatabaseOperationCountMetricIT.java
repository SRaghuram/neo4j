/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.configuration.MetricsSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyDoesNotExist;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.dropDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.startDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class CausalClusterDatabaseOperationCountMetricIT
{
    private static final int TIMEOUT = 60;

    @Inject
    private ClusterFactory clusterFactory;

    @Test
    void shouldDatabaseOperationCountsMatch() throws Exception
    {
        // failed counter is tested in DatabaseOperationCountMetricsIT

        // given
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( MetricsSettings.metrics_enabled, TRUE )
                .withSharedCoreParam( MetricsSettings.csv_enabled, TRUE )
                .withSharedCoreParam( MetricsSettings.csv_interval, "1s" );
        var cluster = clusterFactory.createCluster( clusterConfig );

        // when start
        cluster.start();
        // then
        assertDatabaseEventuallyStarted( "system", cluster );
        assertDatabaseEventuallyStarted( "neo4j", cluster );
        assertDatabaseCount( cluster, 2, 2, 0, 0 );

        // when create
        createDatabase( "foo", cluster );
        createDatabase( "bar", cluster );
        // then
        assertDatabaseEventuallyStarted( "foo", cluster );
        assertDatabaseEventuallyStarted( "bar", cluster );
        assertDatabaseCount( cluster, 4, 4, 0, 0 );

        // when stop
        stopDatabase( "foo", cluster );
        // then
        assertDatabaseEventuallyStopped(  "foo", cluster );
        assertDatabaseCount( cluster, 4, 4, 1, 0 );

        // when drop
        dropDatabase( "bar", cluster );
        // then
        assertDatabaseEventuallyDoesNotExist( "bar", cluster );
        assertDatabaseCount( cluster, 4, 4,2, 1 );

        // when start
        startDatabase( "foo", cluster );
        // then
        assertDatabaseEventuallyStarted( "foo", cluster );
        assertDatabaseCount( cluster, 4,5, 2, 1 );
    }

    private static void assertDatabaseCount( Cluster cluster, long create, long start, long stop, long drop )
    {
        cluster.allMembers().forEach( member ->
                                      {
                                          try
                                          {
                                              assertMetricsEqual( member, "neo4j.db.operation.count.create", create );
                                              assertMetricsEqual( member, "neo4j.db.operation.count.start", start );
                                              assertMetricsEqual( member, "neo4j.db.operation.count.stop", stop );
                                              assertMetricsEqual( member, "neo4j.db.operation.count.drop", drop );
                                          }
                                          catch ( InterruptedException e )
                                          {
                                              throw new IllegalStateException( e );
                                          }
                                      } );
    }

    private static void assertMetricsEqual( ClusterMember member, String metricsName, long count ) throws InterruptedException
    {
        var metricsDir = member.homePath().resolve( MetricsSettings.csv_path.defaultValue().toString() );
        File file = metricsCsv( metricsDir.toFile(), metricsName );
        assertEventually( () -> readValue( file ), t ->  t == count, TIMEOUT, SECONDS );
    }

    private static Long readValue( File file )
    {
        try
        {
            return readLongCounterAndAssert( file, -1, ( one, two ) -> true );
        }
        catch ( IOException io )
        {
            throw new UncheckedIOException( io );
        }
    }
}
