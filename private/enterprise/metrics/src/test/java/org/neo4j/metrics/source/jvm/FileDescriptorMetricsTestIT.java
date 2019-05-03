/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.jvm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Objects;
import java.util.function.BiPredicate;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongGaugeAndAssert;
import static org.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( TestDirectoryExtension.class )
class FileDescriptorMetricsTestIT
{
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;
    private File metricsFolder;

    @BeforeEach
    void setUp()
    {
        metricsFolder = testDirectory.directory( "metrics" );

        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setConfig( MetricsSettings.metricsEnabled, "false" )
                .setConfig( MetricsSettings.jvmFileDescriptorsEnabled, "true" )
                .build();
    }

    @AfterEach
    void cleanUp()
    {
        managementService.shutdown();
    }

    @Test
    void shouldMonitorOpenFileDescriptors() throws Exception
    {
        assertEventually( "Some file descriptors are open at startup",
                () -> readLongGaugeValue( metricsCsv( metricsFolder, "neo4j.vm.file.descriptors.count" ) ), greaterThan( 0L ), 30, SECONDS );
    }

    @Test
    void shouldMonitorMaximumFileDescriptors() throws Exception
    {
        assertEventually( "Some file descriptors are open at startup",
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
                } ), greaterThan( 0L ), 30, SECONDS );
    }
}
