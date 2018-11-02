/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.upstream.strategies;

import java.util.Optional;
import java.util.Random;

import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import org.neo4j.helpers.Service;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
public class ConnectToRandomCoreServerStrategy extends UpstreamDatabaseSelectionStrategy
{
    public static final String IDENTITY = "connect-to-random-core-server";
    private final Random random = new Random();

    public ConnectToRandomCoreServerStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public Optional<MemberId> upstreamDatabase() throws UpstreamDatabaseSelectionException
    {
        final CoreTopology coreTopology = topologyService.localCoreServers();

        if ( coreTopology.members().size() == 0 )
        {
            throw new UpstreamDatabaseSelectionException( "No core servers available" );
        }

        int skippedServers = random.nextInt( coreTopology.members().size() );

        return coreTopology.members().keySet().stream().skip( skippedServers ).findFirst();
    }
}
