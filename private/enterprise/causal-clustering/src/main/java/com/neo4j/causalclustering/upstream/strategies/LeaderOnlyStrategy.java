/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.database.DatabaseId;

@ServiceProvider
public class LeaderOnlyStrategy extends UpstreamDatabaseSelectionStrategy
{
    public static final String IDENTITY = "leader-only";

    public LeaderOnlyStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public Optional<MemberId> upstreamMemberForDatabase( DatabaseId databaseId ) throws UpstreamDatabaseSelectionException
    {
        Set<MemberId> coreMemberIds = topologyService.coreTopologyForDatabase( databaseId ).members().keySet();

        if ( coreMemberIds.isEmpty() )
        {
            throw new UpstreamDatabaseSelectionException( "No core servers available" );
        }

        for ( MemberId memberId : coreMemberIds )
        {
            RoleInfo role = topologyService.role( databaseId, memberId );

            if ( role == RoleInfo.LEADER && !Objects.equals( myself, memberId ) )
            {
                return Optional.of( memberId );
            }
        }

        return Optional.empty();
    }
}
