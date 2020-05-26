/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.global;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.metrics.source.server.ServerMetrics;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import org.neo4j.common.DependencySatisfier;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.extension.context.GlobalExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.factory.DbmsInfo.COMMUNITY;

@TestDirectoryExtension
class GlobalMetricsBuilderTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldAddServerMetricsWhenServerEnabled()
    {
        testBuildingWithServerMetrics( true );
    }

    @Test
    void shouldNotAddServerMetricsWhenServerDisabled()
    {
        testBuildingWithServerMetrics( false );
    }

    private void testBuildingWithServerMetrics( boolean serverMetricsEnabled )
    {
        Config config = configWithServerMetrics( serverMetricsEnabled );
        ExtensionContext extensionContext = new GlobalExtensionContext( Neo4jLayout.of( config ), COMMUNITY, mock( DependencySatisfier.class ) );
        LifeSupport life = new LifeSupport();

        GlobalMetricsExporter exporter = new GlobalMetricsExporter( new MetricRegistry(), config,
                extensionContext, mock( GlobalMetricsExtensionFactory.Dependencies.class ), life );

        exporter.export();

        var containsServerMetrics = new Condition<>( item -> item instanceof ServerMetrics, "instance of ServerMetrics" );
        if ( serverMetricsEnabled )
        {
            assertThat( life.getLifecycleInstances() ).haveAtLeastOne( containsServerMetrics );
        }
        else
        {
            assertThat( life.getLifecycleInstances() ).doesNotHave( containsServerMetrics );
        }
    }

    private static Config configWithServerMetrics( boolean enabled )
    {
        return Config.newBuilder()
                .set( HttpConnector.enabled, enabled )
                .set( MetricsSettings.neoServerEnabled, true )
                .build();
    }
}
