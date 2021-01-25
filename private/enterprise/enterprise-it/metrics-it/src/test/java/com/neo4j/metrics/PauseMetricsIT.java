/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.configuration.MetricsSettings;
import com.neo4j.configuration.OnlineBackupSettings;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;

import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.VmPauseMonitor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.test.assertion.Assert.assertEventually;

@DbmsExtension( configurationCallback = "configure" )
class PauseMetricsIT
{
    @Inject
    private TestDirectory testDirectory;

    private Path metricsDirectory;
    private VmPauseMonitor.Monitor monitor;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        metricsDirectory = testDirectory.directory( "metrics" );
        builder.setConfig( MetricsSettings.metrics_enabled, true )
               .setConfig( MetricsSettings.metrics_filter, GlobbingPattern.create( "*pause_time*" ) )
               .setConfig( MetricsSettings.csv_enabled, true )
               .setConfig( MetricsSettings.csv_interval, Duration.ofMillis( 10 ) )
               .setConfig( MetricsSettings.csv_path, metricsDirectory.toAbsolutePath() )
               .setConfig( OnlineBackupSettings.online_backup_enabled, false );
        Monitors monitors = new Monitors();
        monitor = monitors.newMonitor( VmPauseMonitor.Monitor.class );
        builder.setMonitors( monitors );
    }

    @Test
    void pauseMetrics()
    {
        monitor.pauseDetected( new VmPauseMonitor.VmPauseInfo( 10, 0, 0 ) );

        var greaterThanZero = new Condition<Long>( value -> value > 0, "Greater than zero" );

        assertEventually( "Metrics report should include vm pause time",
                          () -> readLongGaugeValue( metricsCsv( metricsDirectory, "neo4j.vm.pause_time" ) ),
                          greaterThanZero, 5, SECONDS );
    }
}
