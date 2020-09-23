/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.MetricsSettings.metrics_filter;
import static com.neo4j.configuration.MetricsSettings.metrics_namespaces_enabled;
import static com.neo4j.configuration.MetricsSettings.metrics_prefix;
import static com.neo4j.configuration.MetricsSettings.neo_transaction_logs_enabled;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

@TestDirectoryExtension
class MetricsSettingsMigratorTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void oldLogMetricsSettingShouldBeMigratedToMetricsFilter() throws IOException
    {
        Path confFile = testDirectory.createFile( "neo4j.conf" );
        // Migration from metrics.neo4j.logrotation.enabled to metrics.neo4j.logs.enabled should
        // still work with migration to metrics filter
        Files.write( confFile, asList( metrics_filter.name() + "=",
                "metrics.neo4j.logrotation.enabled=true" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();

        assertThat( config.get( metrics_filter ) ).containsExactlyElementsOf( GlobbingPattern.create( "neo4j.*.log.*" ) );
    }

    @Test
    void useOfDeprecatedMetricsSettingShouldPrintWarning() throws IOException
    {
        Path confFile = testDirectory.createFile( "neo4j.conf" );
        Files.write( confFile, asList( metrics_filter.name() + "=my.filter.string",
                "metrics.neo4j.logs.enabled=true" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThat( logProvider ).forClass( Config.class ).forLevel( WARN )
                .containsMessageWithArguments( "Use of deprecated setting %s. It is replaced by %s", neo_transaction_logs_enabled.name(),
                        metrics_filter.name() );
    }

    @Test
    void suppliedMetricsFilterShouldNotBeOverwrittenJustAppended() throws IOException
    {
        Path confFile = testDirectory.createFile( "neo4j.conf" );
        Files.write( confFile, asList( metrics_filter.name() + "=my.filter.string",
                "metrics.neo4j.logs.enabled=true" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();

        assertThat( config.get( metrics_filter ) ).containsExactlyElementsOf( GlobbingPattern.create( "neo4j.*.log.*", "my.filter.string" ) );
    }

    @Test
    void defaultMetricsFilterShouldNotBeOverwrittenIfNoFilterSuppliedJustAppended() throws IOException
    {
        Path confFile = testDirectory.createFile( "neo4j.conf" );
        Files.write( confFile, singletonList( "metrics.neo4j.pagecache.enabled=true" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();

        List<GlobbingPattern> expected = GlobbingPattern.create( "neo4j.page_cache.*" );
        expected.addAll( metrics_filter.defaultValue() );
        assertThat( config.get( metrics_filter ) ).containsExactlyElementsOf( expected );
    }

    @Test
    void metricsFilterWithNamespacesEnabled() throws IOException
    {
        Path confFile = testDirectory.createFile( "neo4j.conf" );
        Files.write( confFile, asList( metrics_prefix.name() + "=prefix",
                metrics_namespaces_enabled.name() + "=true",
                metrics_filter.name() + "=",
                "metrics.neo4j.counts.enabled=true",
                "metrics.neo4j.pagecache.enabled=true" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();

        assertThat( config.get( metrics_filter ) )
                .containsExactlyElementsOf( GlobbingPattern.create( "prefix.dbms.page_cache.*", "prefix.database.*.ids_in_use.*" ) );
    }

    @Test
    void metricsFilterWithNamespacesDisabled() throws IOException
    {
        Path confFile = testDirectory.createFile( "neo4j.conf" );
        Files.write( confFile, asList( metrics_prefix.name() + "=prefix",
                metrics_namespaces_enabled.name() + "=false",
                metrics_filter.name() + "=",
                "metrics.neo4j.counts.enabled=true",
                "metrics.neo4j.pagecache.enabled=true" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();

        assertThat( config.get( metrics_filter ) ).containsExactlyElementsOf( GlobbingPattern.create( "prefix.page_cache.*", "prefix.*.ids_in_use.*" ) );
    }
}
