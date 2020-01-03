/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Optional;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
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
    protected MemberId myself;
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
    public void inject( TopologyService topologyService, Config config, LogProvider logProvider, MemberId myself )
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

    public abstract Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException;

    @Override
    public String toString()
    {
        return name;
    }
}
