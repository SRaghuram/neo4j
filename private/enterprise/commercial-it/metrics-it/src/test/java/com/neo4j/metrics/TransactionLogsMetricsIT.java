package com.neo4j.metrics;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.extension.CommercialDbmsExtension;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

@CommercialDbmsExtension( configurationCallback = "configure" )
class TransactionLogsMetricsIT
{
    @Inject
    private TestDirectory directory;
    @Inject
    private GraphDatabaseAPI db;
    private File outputPath;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        outputPath = new File( directory.storeDir(), "metrics" );
        builder.setConfig( MetricsSettings.metricsEnabled, TRUE );
        builder.setConfig( MetricsSettings.csvEnabled, TRUE );
        builder.setConfig( preallocate_logical_logs, FALSE );
        builder.setConfig( MetricsSettings.csvPath, outputPath.getAbsolutePath() );
        builder.setConfig( check_point_interval_time, "100ms" );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, FALSE );
    }

    @Test
    void reportTransactionLogsAppendedBytesEqualToFileSizeWhenPreallocationDisabled() throws Exception
    {
        addNodes( 100 );
        File metricsFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.appended_bytes" );

        LogFiles logFiles = db.getDependencyResolver().resolveDependency( LogFiles.class );
        long fileLength = logFiles.getHighestLogFile().length();

        assertEventually( "Metrics report should include correct number of written transaction log bytes.", () -> readLongCounterValue( metricsFile ),
                equalTo( fileLength ), 1, MINUTES );
    }

    @Test
    void reportTransactionLogsRotations() throws Exception
    {
        LogRotation logRotation = db.getDependencyResolver().resolveDependency( LogRotation.class );
        addNodes( 1 );
        logRotation.rotateLogFile();
        addNodes( 1 );
        logRotation.rotateLogFile();
        addNodes( 1 );

        File rotationEvents = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.rotation_events" );
        File rotationTime = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.rotation_total_time" );
        File rotationDuration = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.rotation_duration" );

        assertEventually( "Metrics report should include correct number of reported rotations.",
                () -> readLongCounterValue( rotationEvents ), equalTo( 2L ), 1, MINUTES );

        long rotationTotalTimeValue = readLongCounterAndAssert( rotationTime, ( newValue, currentValue ) -> newValue >= currentValue );
        assertThat( rotationTotalTimeValue, greaterThanOrEqualTo( 0L ) );
        long rotationDurationValue = readLongGaugeValue( rotationDuration );
        assertThat( rotationDurationValue, greaterThanOrEqualTo( 0L ) );
    }

    private void addNodes( int numberOfNodes )
    {
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( Label.label( "Label" ) );
                node.setProperty( "name", randomAlphabetic( 256 ) );
                tx.success();
            }
        }
    }
}
