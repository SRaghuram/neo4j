/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.configuration.ServerGroupsSupplier;

import java.util.Collection;
import java.util.Optional;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

@ServiceProvider
public class ConnectRandomlyWithinServerGroupStrategy extends UpstreamDatabaseSelectionStrategy
{
    private static final String IDENTITY = "connect-randomly-within-server-group";

    private ConnectRandomlyToServerGroupImpl strategyImpl;

    public ConnectRandomlyWithinServerGroupStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public void init()
    {
        var serverGroupsProvider = ServerGroupsSupplier.listen( config );
        strategyImpl = new ConnectRandomlyToServerGroupImpl( serverGroupsProvider, topologyService, myself );
        log.warn( "Upstream selection strategy " + name + " is deprecated. Consider using " + IDENTITY + " instead." );
    }

    @Override
    public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return strategyImpl.upstreamServerForDatabase( namedDatabaseId );
    }

    @Override
    public Collection<ServerId> upstreamServersForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return strategyImpl.upstreamServersForDatabase( namedDatabaseId );
    }
}
