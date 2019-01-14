/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readDoubleGaugeValue;
import static org.neo4j.metrics.MetricsTestHelper.readLongCounterValue;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_EVICTIONS;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_EVICTION_EXCEPTIONS;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_FLUSHES;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_HITS;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_HIT_RATIO;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_PAGE_FAULTS;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_PINS;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_UNPINS;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_USAGE_RATIO;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( TestDirectoryExtension.class )
class PageCacheMetricsIT
{
    @Inject
    private TestDirectory testDirectory;
    private File metricsDirectory;
    private GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        metricsDirectory = testDirectory.directory( "metrics" );
        database = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.storeDir() )
                .setConfig( MetricsSettings.metricsEnabled, Settings.FALSE  )
                .setConfig( MetricsSettings.neoPageCacheEnabled, Settings.TRUE  )
                .setConfig( MetricsSettings.csvEnabled, Settings.TRUE )
                .setConfig( MetricsSettings.csvInterval, "100ms" )
                .setConfig( MetricsSettings.csvPath, metricsDirectory.getAbsolutePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void pageCacheMetrics() throws Exception
    {
        Label testLabel = Label.label( "testLabel" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( testLabel );
            node.setProperty( "property", "value" );
            transaction.success();
        }

        try ( Transaction ignored = database.beginTx() )
        {
            ResourceIterator<Node> nodes = database.findNodes( testLabel );
            assertEquals( 1, nodes.stream().count() );
        }

        assertMetrics( "Metrics report should include page cache pins", PC_PINS, greaterThan( 0L ) );
        assertMetrics( "Metrics report should include page cache unpins", PC_UNPINS, greaterThan( 0L ) );
        assertMetrics( "Metrics report should include page cache evictions", PC_EVICTIONS, greaterThanOrEqualTo( 0L ) );
        assertMetrics( "Metrics report should include page cache page faults", PC_PAGE_FAULTS, greaterThan( 0L ) );
        assertMetrics( "Metrics report should include page cache hits", PC_HITS, greaterThan( 0L ) );
        assertMetrics( "Metrics report should include page cache flushes", PC_FLUSHES, greaterThanOrEqualTo( 0L ) );
        assertMetrics( "Metrics report should include page cache exceptions", PC_EVICTION_EXCEPTIONS, equalTo( 0L ) );

        assertEventually(
                "Metrics report should include page cache hit ratio",
                () -> readDoubleGaugeValue( metricsCsv( metricsDirectory, PC_HIT_RATIO ) ),
                lessThanOrEqualTo( 1.0 ),
                5, SECONDS );

        assertEventually(
                "Metrics report should include page cache usage ratio",
                () -> readDoubleGaugeValue( metricsCsv( metricsDirectory, PC_USAGE_RATIO ) ),
                lessThanOrEqualTo( 1.0 ),
                5, SECONDS );
    }

    private void assertMetrics( String message, String metricName, Matcher<Long> matcher ) throws Exception
    {
        assertEventually( message, () -> readLongCounterValue( metricsCsv( metricsDirectory, metricName ) ), matcher, 5, SECONDS );
    }
}
