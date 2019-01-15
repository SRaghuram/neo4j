/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.global;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.common.DependencySatisfier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.extension.context.GlobalExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.metrics.source.server.ServerMetrics;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.COMMUNITY;

@ExtendWith( TestDirectoryExtension.class )
class GlobalMetricsBuilderTest
{
    @Inject
    private TestDirectory testDir;

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
        ExtensionContext extensionContext = new GlobalExtensionContext( testDir.storeLayout(), COMMUNITY, mock( DependencySatisfier.class ) );
        LifeSupport life = new LifeSupport();

        GlobalMetricsExporter exporter = new GlobalMetricsExporter( new MetricRegistry(), config, NullLogService.getInstance(),
                extensionContext, mock( GlobalMetricsExtensionFactory.Dependencies.class ), life );

        exporter.export();

        if ( serverMetricsEnabled )
        {
            assertThat( life.getLifecycleInstances(), hasItem( instanceOf( ServerMetrics.class ) ) );
        }
        else
        {
            assertThat( life.getLifecycleInstances(), not( hasItem( instanceOf( ServerMetrics.class ) ) ) );
        }
    }

    private static Config configWithServerMetrics( boolean enabled )
    {
        return Config.builder()
                .withSetting( new HttpConnector( "http" ).enabled, Boolean.toString( enabled ) )
                .withSetting( MetricsSettings.neoServerEnabled, TRUE )
                .build();
    }
}
