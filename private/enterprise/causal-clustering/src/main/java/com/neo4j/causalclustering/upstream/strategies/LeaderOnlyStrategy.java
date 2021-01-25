/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

@ServiceProvider
public class LeaderOnlyStrategy extends UpstreamDatabaseSelectionStrategy
{
    public static final String IDENTITY = "leader-only";

    public LeaderOnlyStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
    {
        Set<ServerId> coreServerIds = topologyService.coreTopologyForDatabase( namedDatabaseId ).servers().keySet();

        if ( coreServerIds.isEmpty() )
        {
            throw new UpstreamDatabaseSelectionException( "No core servers available" );
        }

        for ( ServerId serverId : coreServerIds )
        {
            RoleInfo role = topologyService.lookupRole( namedDatabaseId, serverId );

            if ( role == RoleInfo.LEADER && !Objects.equals( myself, serverId ) )
            {
                return Optional.of( serverId );
            }
        }

        return Optional.empty();
    }
}
