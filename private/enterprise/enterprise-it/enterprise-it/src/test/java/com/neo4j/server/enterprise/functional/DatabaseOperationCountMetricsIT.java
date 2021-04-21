/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.functional;

import com.neo4j.configuration.MetricsSettings;
import com.neo4j.dbms.LocalDbmsOperator;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.test.assertion.Assert.assertEventually;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class DatabaseOperationCountMetricsIT
{
    private static final int TIMEOUT = 180;

    @Inject
    private TestDirectory directory;

    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseAPI graphDatabaseAPI;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( MetricsSettings.metrics_enabled, true )
                .setConfig( MetricsSettings.metrics_filter, GlobbingPattern.create( "*" ) )
                .setConfig( MetricsSettings.csv_enabled, true )
                .setConfig( MetricsSettings.csv_interval, Duration.ofSeconds( 1 ) );
    }

    @Test
    void shouldDatabaseOperationCountsMatch() throws IOException
    {
        // although EnterpriseDbmsExtension gives you a database in each test method, this test does not use that
        // but this created database must be considered in the counts so beside the two built in databases there is a third, that is the reason why
        // create and start count start with 3

        // given
        Path metrics = directory.file( "metrics" );

        var fooTrxLog = directory.homePath().resolve( "data/transactions/foo/neostore.transaction.db.0" );
        var fooTrxLogRenamed = directory.homePath().resolve( "data/transactions/temp" );

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
        Files.move( fooTrxLog, fooTrxLogRenamed );
        systemDatabase.executeTransactionally( "START DATABASE foo" );
        // then
        assertDatabaseCount( metrics, 5, 5, 2, 1, 1, 0 );

        // when heal and start
        Files.move( fooTrxLogRenamed, fooTrxLog );
        var fooId = databaseId( "foo", graphDatabaseAPI );
        graphDatabaseAPI.getDependencyResolver().resolveDependency( LocalDbmsOperator.class ).startDatabase( fooId.get() );
        // then
        assertDatabaseCount( metrics, 5, 6, 2, 1, 1, 1 );
    }

    private void assertDatabaseCount( Path metrics, long create, long start, long stop, long drop, long failed, long recovered )
    {
        assertMetricsEqual( metrics, "neo4j.db.operation.count.create", create );
        assertMetricsEqual( metrics, "neo4j.db.operation.count.start", start );
        assertMetricsEqual( metrics, "neo4j.db.operation.count.stop", stop );
        assertMetricsEqual( metrics, "neo4j.db.operation.count.drop", drop );
        assertMetricsEqual( metrics, "neo4j.db.operation.count.failed", failed );
        assertMetricsEqual( metrics, "neo4j.db.operation.count.recovered", recovered );
    }

    private static void assertMetricsEqual( Path metricsPath, String metricsName, long count )
    {
        Path file = metricsCsv( metricsPath, metricsName );
        assertEventually( () -> readValue( file ), x -> x == count, TIMEOUT, SECONDS );
    }

    private static Long readValue( Path file )
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

    private static Optional<NamedDatabaseId> databaseId( String databaseName, GraphDatabaseAPI gdb )
    {
        var resolver = gdb.getDependencyResolver();
        var databaseManager = resolver.resolveDependency( DatabaseManager.class );
        var idRepo = databaseManager.databaseIdRepository();
        return idRepo.getByName( databaseName );
    }
}
