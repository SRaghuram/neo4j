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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class LeaderOnlyStrategy extends UpstreamDatabaseSelectionStrategy
{
    public static final String IDENTITY = "leader-only";

    public LeaderOnlyStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public Optional<MemberId> upstreamDatabase() throws UpstreamDatabaseSelectionException
    {
        Map<MemberId,RoleInfo> memberRoles = topologyService.allCoreRoles();

        if ( memberRoles.size() == 0 )
        {
            throw new UpstreamDatabaseSelectionException( "No core servers available" );
        }

        for ( Map.Entry<MemberId,RoleInfo> entry : memberRoles.entrySet() )
        {
            RoleInfo role = entry.getValue();
            if ( role == RoleInfo.LEADER && !Objects.equals( myself, entry.getKey() ) )
            {
                return Optional.of( entry.getKey() );
            }
        }

        return Optional.empty();
    }
}
