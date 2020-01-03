/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.function.Predicate.not;

@ServiceProvider
public class TypicallyConnectToRandomReadReplicaStrategy extends UpstreamDatabaseSelectionStrategy
{
    public static final String IDENTITY = "typically-connect-to-random-read-replica";
    private final ModuloCounter counter;

    public TypicallyConnectToRandomReadReplicaStrategy()
    {
        this( 10 );
    }

    public TypicallyConnectToRandomReadReplicaStrategy( int connectToCoreInterval )
    {
        super( IDENTITY );
        this.counter = new ModuloCounter( connectToCoreInterval );
    }

    @Override
    public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId )
    {
        if ( counter.shouldReturnCoreMemberId() )
        {
            return randomCoreMember( namedDatabaseId );
        }
        else
        {
            // shuffled members
            List<MemberId> readReplicaMembers = new ArrayList<>( topologyService.readReplicaTopologyForDatabase( namedDatabaseId ).members().keySet() );
            Collections.shuffle( readReplicaMembers );

            List<MemberId> coreMembers = new ArrayList<>( topologyService.coreTopologyForDatabase( namedDatabaseId ).members().keySet() );
            Collections.shuffle( coreMembers );

            return Stream.concat( readReplicaMembers.stream(), coreMembers.stream() ).filter( not( myself::equals ) ).findFirst();
        }
    }

    private Optional<MemberId> randomCoreMember( NamedDatabaseId namedDatabaseId )
    {
        List<MemberId> coreMembersNotSelf = topologyService.coreTopologyForDatabase( namedDatabaseId )
                .members().keySet().stream()
                .filter( not( myself::equals ) )
                .collect( Collectors.toList() );

        if ( coreMembersNotSelf.isEmpty() )
        {
            return Optional.empty();
        }

        int randomIndex = ThreadLocalRandom.current().nextInt( coreMembersNotSelf.size() );
        return Optional.of( coreMembersNotSelf.get( randomIndex ) );
    }

    private static class ModuloCounter
    {
        private final int modulo;
        private int counter;

        ModuloCounter( int modulo )
        {
            this.modulo = modulo;
        }

        boolean shouldReturnCoreMemberId()
        {
            counter = (counter + 1) % modulo;
            return counter == 0;
        }
    }
}
