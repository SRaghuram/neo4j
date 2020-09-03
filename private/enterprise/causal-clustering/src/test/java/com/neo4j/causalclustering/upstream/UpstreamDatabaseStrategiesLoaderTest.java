/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.IdFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.configuration.CausalClusteringSettings.upstream_selection_strategy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class UpstreamDatabaseStrategiesLoaderTest
{
    private ServerId myself = IdFactory.randomServerId();

    @Test
    void shouldReturnConfiguredClassesOnly()
    {
        // given
        Config config = Config.defaults( upstream_selection_strategy, List.of( "dummy" ) );

        UpstreamDatabaseStrategiesLoader strategies =
                new UpstreamDatabaseStrategiesLoader( mock( TopologyService.class ), config, myself, NullLogProvider.getInstance() );

        // when
        Set<UpstreamDatabaseSelectionStrategy> upstreamDatabaseSelectionStrategies = asSet( strategies.iterator() );

        // then
        assertEquals( 1, upstreamDatabaseSelectionStrategies.size() );
        assertEquals( UpstreamDatabaseStrategySelectorTest.DummyUpstreamDatabaseSelectionStrategy.class,
                upstreamDatabaseSelectionStrategies.stream().map( UpstreamDatabaseSelectionStrategy::getClass ).findFirst().get() );
    }

    @Test
    void shouldReturnTheFirstStrategyThatWorksFromThoseConfigured()
    {
        // given
        Config config = Config.defaults( upstream_selection_strategy, List.of( "yet-another-dummy", "dummy", "another-dummy" ) );

        // when
        UpstreamDatabaseStrategiesLoader strategies =
                new UpstreamDatabaseStrategiesLoader( mock( TopologyService.class ), config, myself, NullLogProvider.getInstance() );

        // then
        assertEquals( UpstreamDatabaseStrategySelectorTest.YetAnotherDummyUpstreamDatabaseSelectionStrategy.class, strategies.iterator().next().getClass() );
    }
}
