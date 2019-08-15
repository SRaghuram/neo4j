/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterValue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.PortUtils.getBoltPort;
import static org.neo4j.test.assertion.Assert.assertEventually;

@DbmsExtension( configurationCallback = "configure" )
class BoltMetricsIT
{
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private GraphDatabaseAPI db;

    private final TransportTestUtil util = new TransportTestUtil();
    private File metricsFolder;
    private TransportConnection conn;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        metricsFolder = testDirectory.directory( "metrics" );
        builder.setConfig( BoltConnector.enabled, true )
            .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
            .setConfig( GraphDatabaseSettings.auth_enabled, false )
            .setConfig( MetricsSettings.metricsEnabled, true )
            .setConfig( MetricsSettings.boltMessagesEnabled, true )
            .setConfig( MetricsSettings.csvEnabled, true )
            .setConfig( MetricsSettings.csvInterval, Duration.ofMillis( 100 ) )
            .setConfig( MetricsSettings.csvPath, metricsFolder.toPath().toAbsolutePath() )
            .setConfig( OnlineBackupSettings.online_backup_enabled, false );
    }

    @AfterEach
    void cleanup() throws Exception
    {
        conn.disconnect();
    }

    @Test
    void shouldMonitorBolt() throws Throwable
    {
        // When
        conn = new SocketConnection()
                .connect( new HostnamePort( "localhost", getBoltPort( db ) ) )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "scheme", "basic", "principal", "neo4j", "credentials", "neo4j" ) ) );

        // Then
        assertEventually( "session shows up as started",
                () -> readLongCounterValue( metricsCsv( metricsFolder, "neo4j.bolt.sessions_started" ) ), equalTo( 1L ), 5, SECONDS );
        assertEventually( "init request shows up as received",
                () -> readLongCounterValue( metricsCsv( metricsFolder, "neo4j.bolt.messages_received" ) ), equalTo( 1L ), 5, SECONDS );
        assertEventually( "init request shows up as started",
                () -> readLongCounterValue( metricsCsv( metricsFolder, "neo4j.bolt.messages_started" ) ), equalTo( 1L ), 5, SECONDS );
        assertEventually( "init request shows up as done",
                () -> readLongCounterValue( metricsCsv( metricsFolder, "neo4j.bolt.messages_done" ) ), equalTo( 1L ), 5, SECONDS );

        assertEventually( "queue time shows up",
                () -> readLongCounterValue( metricsCsv( metricsFolder, "neo4j.bolt.accumulated_queue_time" ) ),
                greaterThanOrEqualTo( 0L ), 5, SECONDS );
        assertEventually( "processing time shows up",
                () -> readLongCounterValue( metricsCsv( metricsFolder, "neo4j.bolt.accumulated_processing_time" ) ),
                greaterThanOrEqualTo( 0L ), 5, SECONDS );
    }
}
