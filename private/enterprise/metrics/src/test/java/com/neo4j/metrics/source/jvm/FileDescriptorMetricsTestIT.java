/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.jvm;

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import org.apache.commons.lang3.SystemUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Objects;
import java.util.function.BiPredicate;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeAndAssert;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.neo4j.test.assertion.Assert.assertEventually;

@TestDirectoryExtension
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

        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setConfig( MetricsSettings.metricsEnabled, true )
                .setConfig( MetricsSettings.jvmFileDescriptorsEnabled, true )
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
                () -> readLongGaugeValue( metricsCsv( metricsFolder, "neo4j.vm.file.descriptors.count" ) ), new OSDependentMatcher(), 30, SECONDS );
    }

    @Test
    void shouldMonitorMaximumFileDescriptors() throws Exception
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
                } ), new OSDependentMatcher(), 30, SECONDS );
    }

    private static class OSDependentMatcher extends BaseMatcher<Long>
    {
        private final Matcher<Long> matcher;

        private OSDependentMatcher()
        {
            if ( SystemUtils.IS_OS_UNIX )
            {
                matcher = greaterThan( 0L );
            }
            else
            {
                matcher = comparesEqualTo( -1L );
            }
        }

        @Override
        public boolean matches( Object actual )
        {
            return matcher.matches( actual );
        }

        @Override
        public void describeTo( Description description )
        {
            matcher.describeTo( description );
        }
    }
}
