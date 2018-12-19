/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongCounterValue;
import static org.neo4j.test.PortUtils.getBoltPort;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( TestDirectoryExtension.class )
class BoltMetricsIT
{
    @Inject
    private TestDirectory testDirectory;

    private final TransportTestUtil util = new TransportTestUtil( new Neo4jPackV1() );

    private GraphDatabaseAPI db;
    private TransportConnection conn;

    @AfterEach
    void cleanup() throws Exception
    {
        conn.disconnect();
        db.shutdown();
    }

    @Test
    void shouldMonitorBolt() throws Throwable
    {
        // Given
        File metricsFolder = testDirectory.directory( "metrics" );
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.databaseDir() )
                .setConfig( new BoltConnector( "bolt" ).type, "BOLT" )
                .setConfig( new BoltConnector( "bolt" ).enabled, "true" )
                .setConfig( new BoltConnector( "bolt" ).listen_address, "localhost:0" )
                .setConfig( GraphDatabaseSettings.auth_enabled, "false" )
                .setConfig( MetricsSettings.metricsEnabled, "false" )
                .setConfig( MetricsSettings.boltMessagesEnabled, "true" )
                .setConfig( MetricsSettings.csvEnabled, "true" )
                .setConfig( MetricsSettings.csvInterval, "100ms" )
                .setConfig( MetricsSettings.csvPath, metricsFolder.getAbsolutePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();

        // When
        conn = new SocketConnection()
                .connect( new HostnamePort( "localhost", getBoltPort( db ) ) )
                .send( util.acceptedVersions( 1, 0, 0, 0 ) )
                .send( util.chunk( new InitMessage( "TestClient",
                        map("scheme", "basic", "principal", "neo4j", "credentials", "neo4j") ) ) );

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
