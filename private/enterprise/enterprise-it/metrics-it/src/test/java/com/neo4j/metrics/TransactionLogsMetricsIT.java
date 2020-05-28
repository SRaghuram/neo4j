/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.configuration.MetricsSettings;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterValue;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class TransactionLogsMetricsIT
{
    @Inject
    private TestDirectory directory;
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private DatabaseManagementService managementService;
    @Inject
    private LogFiles logFiles;
    private File outputPath;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        outputPath = new File( directory.homeDir(), "metrics" );
        builder.setConfig( MetricsSettings.metrics_enabled, true );
        builder.setConfig( MetricsSettings.csv_enabled, true );
        builder.setConfig( preallocate_logical_logs, false );
        builder.setConfig( MetricsSettings.csv_path, outputPath.toPath().toAbsolutePath() );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, false );
    }

    @Test
    void reportTransactionLogsAppendedBytesEqualToFileSizeWhenPreallocationDisabled()
    {
        addNodes( 100, db );
        File metricsFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.appended_bytes" );

        long fileLength = logFiles.getHighestLogFile().length();

        assertEventually( "Metrics report should include correct number of written transaction log bytes.", () -> readLongCounterValue( metricsFile ),
                equalityCondition( fileLength ), 1, MINUTES );
    }

    @Test
    void transactionLogsMetricsForDifferentDatabasesAreIndependent()
    {
        String secondDbName = "seconddatabase";
        managementService.createDatabase( secondDbName );
        GraphDatabaseAPI secondDb = (GraphDatabaseAPI) managementService.database( secondDbName );

        addNodes( 100, db );
        long fileLength = logFiles.getHighestLogFile().length();

        File metricsFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.appended_bytes" );
        File secondMetricsFile = metricsCsv( outputPath, "neo4j." + secondDbName + ".log.appended_bytes" );

        assertEventually( "Metrics report should include correct number of written transaction log bytes for default db.",
                () -> readLongCounterValue( metricsFile ), equalityCondition( fileLength ), 1, MINUTES );
        assertEventually( "Metrics report should include correct number of written transaction log bytes for second database.",
                () -> readLongCounterValue( secondMetricsFile ), equalityCondition( (long) CURRENT_FORMAT_LOG_HEADER_SIZE ), 1, MINUTES );

        addNodes( 100, secondDb );

        assertEventually( "Metrics report should include correct number of written transaction log bytes for default db.",
                () -> readLongCounterValue( metricsFile ), equalityCondition( fileLength ), 1, MINUTES );
        assertEventually( "Metrics report should include correct number of written transaction log bytes for second database.",
                () -> readLongCounterValue( secondMetricsFile ), equalityCondition( fileLength ), 1, MINUTES );
    }

    @Test
    void reportTransactionLogsRotations() throws Exception
    {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        LogRotation logRotation = dependencyResolver.resolveDependency( LogRotation.class );
        DatabaseTracer databaseTracer = dependencyResolver.resolveDependency( DatabaseTracer.class );

        try ( TransactionEvent transactionEvent = databaseTracer.beginTransaction( NULL );
                CommitEvent commitEvent = transactionEvent.beginCommitEvent();
                LogAppendEvent logAppendEvent = commitEvent.beginLogAppend() )
        {
            logRotation.rotateLogFile( logAppendEvent );
            logRotation.rotateLogFile( logAppendEvent );
        }

        File rotationEvents = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.rotation_events" );
        File rotationTime = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.rotation_total_time" );
        File rotationDuration = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.rotation_duration" );
        File bytesFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.appended_bytes" );

        assertEventually( "Metrics report should include correct number of reported rotations.",
                () -> readLongCounterValue( rotationEvents ), equalityCondition( 2L ), 1, MINUTES );
        assertEventually( "Metrics report should include correct number of reported bytes written even for header.",
                () -> readLongCounterValue( bytesFile ), equalityCondition( CURRENT_FORMAT_LOG_HEADER_SIZE * 3L ), 1, MINUTES );

        long rotationTotalTimeValue = readLongCounterAndAssert( rotationTime, ( newValue, currentValue ) -> newValue >= currentValue );
        assertThat( rotationTotalTimeValue ).isGreaterThanOrEqualTo( 0 );
        long rotationDurationValue = readLongGaugeValue( rotationDuration );
        assertThat( rotationDurationValue ).isGreaterThanOrEqualTo( 0L );
    }

    private static void addNodes( int numberOfNodes, GraphDatabaseAPI databaseAPI )
    {
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction tx = databaseAPI.beginTx() )
            {
                Node node = tx.createNode( Label.label( "Label" ) );
                node.setProperty( "name", randomAlphabetic( 256 ) );
                tx.commit();
            }
        }
    }
}
