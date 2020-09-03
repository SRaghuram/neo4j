/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.configuration.CausalClusteringSettings;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.service.Services;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.eclipse.collections.impl.block.factory.Predicates.notNull;

/**
 * Loads and initialises any service implementations of <class>UpstreamDatabaseSelectionStrategy</class>.
 * Exposes configured instances of that interface via an iterator.
 */
public class UpstreamDatabaseStrategiesLoader implements Iterable<UpstreamDatabaseSelectionStrategy>
{
    private final TopologyService topologyService;
    private final Config config;
    private final ServerId myself;
    private final Log log;
    private final LogProvider logProvider;

    public UpstreamDatabaseStrategiesLoader( TopologyService topologyService, Config config, ServerId myself, LogProvider logProvider )
    {
        this.topologyService = topologyService;
        this.config = config;
        this.myself = myself;
        this.log = logProvider.getLog( this.getClass() );
        this.logProvider = logProvider;
    }

    @Override
    public Iterator<UpstreamDatabaseSelectionStrategy> iterator()
    {
        final List<String> configuredNames = config.get( CausalClusteringSettings.upstream_selection_strategy );
        final Map<String, UpstreamDatabaseSelectionStrategy> availableStrategies = Services.loadAll( UpstreamDatabaseSelectionStrategy.class ).stream()
                .collect( toMap( UpstreamDatabaseSelectionStrategy::getName, identity() ) );

        final List<UpstreamDatabaseSelectionStrategy> strategies = configuredNames.stream()
                .distinct()
                .map( availableStrategies::get )
                .filter( notNull() )
                .collect( toList() );

        strategies.forEach( strategy ->
        {
            strategy.inject( topologyService, config, logProvider, myself );
        } );

        log.debug( "Upstream database strategies loaded in order of precedence: " + nicelyCommaSeparatedList( strategies ) );
        return strategies.iterator();
    }

    private static String nicelyCommaSeparatedList( Collection<UpstreamDatabaseSelectionStrategy> items )
    {
        return items.stream().map( UpstreamDatabaseSelectionStrategy::toString ).collect( Collectors.joining( ", " ) );
    }
}
