/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.greaterThan;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( TestDirectoryExtension.class )
class StoreSizeMetricsIT
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
                .setConfig( MetricsSettings.metricsEnabled, true )
                .setConfig( MetricsSettings.neoStoreSizeEnabled, true )
                .build();
    }

    @AfterEach
    void cleanUp()
    {
        managementService.shutdown();
    }

    @Test
    void shouldMonitorStoreSizeForAllDatabases() throws Exception
    {
        for ( String name : managementService.listDatabases() )
        {
            String msg = name + " store has some size at startup";
            String metricsName = String.format( "neo4j.%s.store.size.total", name );
            assertEventually( msg, () -> readLongGaugeValue( metricsCsv( metricsFolder, metricsName ) ), greaterThan( 0L ), 1, MINUTES );
        }
    }
}
