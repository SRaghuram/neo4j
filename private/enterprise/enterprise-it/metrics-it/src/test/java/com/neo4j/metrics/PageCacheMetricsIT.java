/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readDoubleGaugeValue;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.test.assertion.Assert.assertEventually;

@DbmsExtension( configurationCallback = "configure" )
class PageCacheMetricsIT
{
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private GraphDatabaseService database;

    private File metricsDirectory;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        metricsDirectory = testDirectory.directory( "metrics" );
        builder.setConfig( MetricsSettings.metricsEnabled, true  )
                .setConfig( MetricsSettings.neoPageCacheEnabled, true  )
                .setConfig( MetricsSettings.csvEnabled, true )
                .setConfig( MetricsSettings.csvInterval, Duration.ofMillis( 10 ) )
                .setConfig( MetricsSettings.csvPath, metricsDirectory.toPath().toAbsolutePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false );
    }

    @Test
    void pageCacheMetrics() throws Exception
    {
        Label testLabel = Label.label( "testLabel" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode( testLabel );
            node.setProperty( "property", "value" );
            transaction.commit();
        }

        try ( Transaction tx = database.beginTx() )
        {
            ResourceIterator<Node> nodes = tx.findNodes( testLabel );
            assertEquals( 1, nodes.stream().count() );
        }

        assertMetrics( "Metrics report should include page cache pins", "neo4j.page_cache.pins", greaterThan( 0L ) );
        assertMetrics( "Metrics report should include page cache unpins", "neo4j.page_cache.unpins", greaterThan( 0L ) );
        assertMetrics( "Metrics report should include page cache evictions", "neo4j.page_cache.evictions", greaterThanOrEqualTo( 0L ) );
        assertMetrics( "Metrics report should include page cache page faults", "neo4j.page_cache.page_faults", greaterThan( 0L ) );
        assertMetrics( "Metrics report should include page cache hits", "neo4j.page_cache.hits", greaterThan( 0L ) );
        assertMetrics( "Metrics report should include page cache flushes", "neo4j.page_cache.flushes", greaterThanOrEqualTo( 0L ) );
        assertMetrics( "Metrics report should include page cache exceptions", "neo4j.page_cache.eviction_exceptions", equalTo( 0L ) );

        assertEventually(
                "Metrics report should include page cache hit ratio",
                () -> readDoubleGaugeValue( metricsCsv( metricsDirectory, "neo4j.page_cache.hit_ratio" ) ),
                lessThanOrEqualTo( 1.0 ),
                5, SECONDS );

        assertEventually(
                "Metrics report should include page cache usage ratio",
                () -> readDoubleGaugeValue( metricsCsv( metricsDirectory, "neo4j.page_cache.usage_ratio" ) ),
                lessThanOrEqualTo( 1.0 ),
                5, SECONDS );
    }

    private void assertMetrics( String message, String metricName, Matcher<Long> matcher ) throws Exception
    {
        assertEventually( message, () -> readLongCounterAndAssert( metricsCsv( metricsDirectory, metricName ), -1, ( a, b ) -> true ), matcher, 5, SECONDS );
    }
}
