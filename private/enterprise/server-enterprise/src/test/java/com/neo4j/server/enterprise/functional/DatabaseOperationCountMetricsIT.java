/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.functional;

import com.neo4j.dbms.LocalDbmsOperator;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

@EnterpriseDbmsExtension
class DatabaseOperationCountMetricsIT
{
    private static final int TIMEOUT = 10;

    @Inject
    private TestDirectory directory;

    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseAPI graphDatabaseAPI;

    @Test
    void shouldDatabaseOperationCountsMatch() throws Throwable
    {
        // although EnterpriseDbmsExtension gives you a database in each test method, this test does not use that
        // but this created database must be considered in the counts so beside the two built in databases there is a third, that is the reason why
        // create and start count start with 3

        // given
        File metrics = directory.file( "metrics" );

        var fooTrxLog = new File( directory.homeDir(), "data/transactions/foo/neostore.transaction.db.0" );
        var fooTrxLogRenamed = new File( directory.homeDir(), "data/transactions/temp" );

        // when start
        var systemDatabase = managementService.database( "system" );
        // then
        assertDatabaseCount( metrics, 3, 3, 0, 0, 0, 0 );

        // when create
        systemDatabase.executeTransactionally( "CREATE DATABASE foo" );
        systemDatabase.executeTransactionally( "CREATE DATABASE bar" );
        // then
        assertDatabaseCount( metrics, 5, 5, 0, 0, 0, 0 );

        // when stop
        systemDatabase.executeTransactionally( "STOP DATABASE foo" );
        // then
        assertDatabaseCount( metrics, 5, 5, 1, 0, 0, 0 );

        // when drop
        systemDatabase.executeTransactionally( "DROP DATABASE bar" );
        // then
        assertDatabaseCount( metrics, 5, 5, 2, 1, 0, 0 );

        // when damage and start
        assertTrue( fooTrxLog.renameTo( fooTrxLogRenamed ) );
        systemDatabase.executeTransactionally( "START DATABASE foo" );
        // then
        assertDatabaseCount( metrics, 5, 5, 2, 1, 1, 0 );

        // when heal and start
        assertTrue( fooTrxLogRenamed.renameTo( fooTrxLog ) );
        graphDatabaseAPI.getDependencyResolver().resolveDependency( LocalDbmsOperator.class ).startDatabase( "foo" );
        // then
        assertDatabaseCount( metrics, 5, 6, 2, 1, 1, 1 );
    }

    private void assertDatabaseCount( File metrics, long create, long start, long stop, long drop, long failed, long recovered ) throws InterruptedException
    {
        assertMetricsEqual( metrics, "neo4j.db.operation.count.create", create );
        assertMetricsEqual( metrics, "neo4j.db.operation.count.start", start );
        assertMetricsEqual( metrics, "neo4j.db.operation.count.stop", stop );
        assertMetricsEqual( metrics, "neo4j.db.operation.count.drop", drop );
        assertMetricsEqual( metrics, "neo4j.db.operation.count.failed", failed );
        assertMetricsEqual( metrics, "neo4j.db.operation.count.recovered", recovered );
    }

    private static void assertMetricsEqual( File metricsPath, String metricsName, long count ) throws InterruptedException
    {
        File file = metricsCsv( metricsPath, metricsName );
        assertEventually( () -> readValue( file ), equalTo( count ), TIMEOUT, SECONDS );
    }

    private static Long readValue( File file ) throws InterruptedException
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
