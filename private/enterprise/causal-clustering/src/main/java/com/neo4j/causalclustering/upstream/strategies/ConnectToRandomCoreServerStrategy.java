/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

@ServiceProvider
public class ConnectToRandomCoreServerStrategy extends UpstreamDatabaseSelectionStrategy
{
    static final String IDENTITY = "connect-to-random-core-server";

    public ConnectToRandomCoreServerStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
    {
        return choices( namedDatabaseId ).findFirst();
    }

    @Override
    public Collection<ServerId> upstreamServersForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
    {
        return choices( namedDatabaseId ).collect( Collectors.toList() );
    }

    private Stream<ServerId> choices( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
    {
        final DatabaseCoreTopology coreTopology = topologyService.coreTopologyForDatabase( namedDatabaseId );

        if ( coreTopology.servers().isEmpty() )
        {
            throw new UpstreamDatabaseSelectionException( "No core servers available" );
        }

        var servers = new ArrayList<>( coreTopology.servers().keySet() );
        Collections.shuffle( servers );
        return servers.stream();
    }
}
