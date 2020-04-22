/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.stream.Collectors;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.NamedMemoryPool;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.csvEnabled;
import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.csvPath;
import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.metricsEnabled;
import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.condition;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
public class MemoryPoolsMetricsIT
{
    @Inject
    private TestDirectory directory;
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private DatabaseManagementService managementService;
    @Inject
    private MemoryPools pools;
    private File outputPath;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        outputPath = new File( directory.homeDir(), "metrics" );
        builder.setConfig( metricsEnabled, true );
        builder.setConfig( csvEnabled, true );
        builder.setConfig( csvPath, outputPath.toPath().toAbsolutePath() );
    }

    @Test
    void writeGlobalPoolsMetrics()
    {
        var globalPools = pools.getPools();
        assertThat( globalPools ).isNotEmpty();
        globalPools.forEach( pool ->
        {
            assertDoesNotThrow( () ->
            {
                var usedHeapReports = metricsCsv( outputPath, buildGlobalMetricFileName( pool, ".used_heap" ) );
                var usedNativeReports = metricsCsv( outputPath, buildGlobalMetricFileName( pool, ".used_native" ) );
                var totalUsedReports = metricsCsv( outputPath, buildGlobalMetricFileName( pool, ".total_used" ) );
                var totalSizeReports = metricsCsv( outputPath, buildGlobalMetricFileName( pool, ".total_size" ) );
                var freeReports = metricsCsv( outputPath, buildGlobalMetricFileName( pool, ".free" ) );

                assertEventually( "Used heap should be reported.",
                        () -> readLongGaugeValue( usedHeapReports ), condition( value -> value >= 0L ), 1, MINUTES );
                assertEventually( "Used native memory should be reported.",
                        () -> readLongGaugeValue( usedNativeReports ), condition( value -> value >= 0L ), 1, MINUTES );
                assertEventually( "Total used memory should be reported.",
                        () -> readLongGaugeValue( totalUsedReports ), condition( value -> value >= 0L ), 1, MINUTES );
                assertEventually( "Total pool size should be reported.",
                        () -> readLongGaugeValue( totalSizeReports ), condition( value -> value >= 0L ), 1, MINUTES );
                assertEventually( "Free memory should be reported.",
                        () -> readLongGaugeValue( freeReports ), condition( value -> value >= 0L ), 1, MINUTES );
            }, "Metrics for pool " + pool.name() + " should be reported." );
        } );
    }

    @Test
    void writeDatabasePoolsMetrics()
    {
        var dbPools = pools.getPools().stream().filter( pool -> pool instanceof GlobalMemoryGroupTracker )
                .flatMap( pool -> ((GlobalMemoryGroupTracker) pool).getDatabasePools().stream() )
                .filter( pool -> db.databaseName().equals( pool.databaseName() ) ).collect( Collectors.toList() );
        assertThat( dbPools ).isNotEmpty();
        dbPools.forEach( pool ->
        {
            assertDoesNotThrow( () ->
            {
                var usedHeapReports = metricsCsv( outputPath, buildDatabaseMetricFileName( pool, db.databaseName(), ".used_heap" ) );
                var usedNativeReports = metricsCsv( outputPath, buildDatabaseMetricFileName( pool, db.databaseName(), ".used_native" ) );
                var totalUsedReports = metricsCsv( outputPath, buildDatabaseMetricFileName( pool, db.databaseName(), ".total_used" ) );
                var totalSizeReports = metricsCsv( outputPath, buildDatabaseMetricFileName( pool, db.databaseName(), ".total_size" ) );
                var freeReports = metricsCsv( outputPath, buildDatabaseMetricFileName( pool, db.databaseName(), ".free" ) );

                assertEventually( "Used heap should be reported.",
                        () -> readLongGaugeValue( usedHeapReports ), condition( value -> value >= 0L ), 1, MINUTES );
                assertEventually( "Used native memory should be reported.",
                        () -> readLongGaugeValue( usedNativeReports ), condition( value -> value >= 0L ), 1, MINUTES );
                assertEventually( "Total used memory should be reported.",
                        () -> readLongGaugeValue( totalUsedReports ), condition( value -> value >= 0L ), 1, MINUTES );
                assertEventually( "Total pool size should be reported.",
                        () -> readLongGaugeValue( totalSizeReports ), condition( value -> value >= 0L ), 1, MINUTES );
                assertEventually( "Free memory should be reported.",
                        () -> readLongGaugeValue( freeReports ), condition( value -> value >= 0L ), 1, MINUTES );
            }, "Metrics for pool " + pool.name() + " should be reported." );
        } );
    }

    private String buildGlobalMetricFileName( NamedMemoryPool pool, String metrics )
    {
        var fileName = "neo4j.dbms.pool." + pool.group() + metrics;
        return fileName.toLowerCase().replace( ' ', '_' );
    }

    private String buildDatabaseMetricFileName( NamedMemoryPool pool, String databaseName, String metrics )
    {
        var fileName = "neo4j." + databaseName + ".pool." + pool.group() + "." + pool.name() + metrics;
        return fileName.toLowerCase().replace( ' ', '_' );
    }
}
