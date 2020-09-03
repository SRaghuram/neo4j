/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.discovery.TopologyService;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.service.NamedService;

@Service
public abstract class UpstreamDatabaseSelectionStrategy implements NamedService
{
    protected TopologyService topologyService;
    protected Config config;
    protected Log log;
    protected ServerId myself;
    protected String name;

    protected UpstreamDatabaseSelectionStrategy( String name )
    {
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    // Service loader can't inject via the constructor
    public void inject( TopologyService topologyService, Config config, LogProvider logProvider, ServerId myself )
    {
        this.topologyService = topologyService;
        this.config = config;
        this.log = logProvider.getLog( this.getClass() );
        this.myself = myself;
        log.info( "Using upstream selection strategy " + name );
        init();
    }

    public void init()
    {
    }

    /**
     * @deprecated Callers of this method should use {@link UpstreamDatabaseSelectionStrategy#firstUpstreamServerForDatabase(NamedDatabaseId)}.
     *   Implementers should override {@link UpstreamDatabaseSelectionStrategy#upstreamServersForDatabase(NamedDatabaseId)} instead.
     *   In future versions this method will first be provided with no-op default implementation, then eventually removed.
     */
    @Deprecated( since = "4.0.3" )
    public abstract Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException;

    public final Optional<ServerId> firstUpstreamServerForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
    {
        return upstreamServersForDatabase( namedDatabaseId ).stream().findFirst();
    }

    public Collection<ServerId> upstreamServersForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
    {
        return upstreamServerForDatabase( namedDatabaseId ).stream().collect( Collectors.toList() );
    }

    @Override
    public String toString()
    {
        return name;
    }
}
