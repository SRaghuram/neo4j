/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.jvm;

import com.neo4j.configuration.MetricsSettings;
import org.apache.commons.lang3.SystemUtils;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeAndAssert;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.test.assertion.Assert.assertEventually;

@TestDirectoryExtension
class FileDescriptorMetricsTestIT
{
    private static final Condition<Long> DESCRIPTOR_CONDITION = new Condition<>( new OSDependentPredicate(), "Descriptor condition." );
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;
    private Path metricsFolder;

    @BeforeEach
    void setUp()
    {
        metricsFolder = testDirectory.directoryPath( "metrics" );

        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( MetricsSettings.metrics_enabled, true )
                .setConfig( MetricsSettings.metrics_filter, GlobbingPattern.create( "*vm.file.descriptors*" ) )
                .setConfig( MetricsSettings.csv_interval, Duration.ofMillis( 10 ) )
                .build();
    }

    @AfterEach
    void cleanUp()
    {
        managementService.shutdown();
    }

    @Test
    void shouldMonitorOpenFileDescriptors()
    {
        assertEventually( "Some file descriptors are open at startup",
                () -> readLongGaugeValue( metricsCsv( metricsFolder, "neo4j.vm.file.descriptors.count" ) ), DESCRIPTOR_CONDITION, 30, SECONDS );
    }

    @Test
    void shouldMonitorMaximumFileDescriptors()
    {
        assertEventually( "Allowed to open files at startup",
                () -> readLongGaugeAndAssert( metricsCsv( metricsFolder, "neo4j.vm.file.descriptors.maximum" ), new BiPredicate<>()
                {
                    private long prevValue = Long.MIN_VALUE;

                    @Override
                    public boolean test( Long newValue, Long currentValue )
                    {
                        if ( prevValue == Long.MIN_VALUE )
                        {
                            prevValue = newValue;
                            return true;
                        }
                        return Objects.equals( newValue, currentValue );
                    }
                } ), DESCRIPTOR_CONDITION, 30, SECONDS );
    }

    private static class OSDependentPredicate implements Predicate<Long>
    {
        @Override
        public boolean test( Long value )
        {
            return SystemUtils.IS_OS_UNIX ? value > 0L : value == -1L;
        }
    }
}
