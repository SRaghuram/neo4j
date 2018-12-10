/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.Service;

import static org.neo4j.function.Predicates.not;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
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
    public Optional<MemberId> upstreamDatabase()
    {
        if ( counter.shouldReturnCoreMemberId() )
        {
            return randomCoreMember();
        }
        else
        {
            // shuffled members
            List<MemberId> readReplicaMembers = new ArrayList<>( topologyService.localReadReplicas().members().keySet() );
            Collections.shuffle( readReplicaMembers );

            List<MemberId> coreMembers = new ArrayList<>( topologyService.localCoreServers().members().keySet() );
            Collections.shuffle( coreMembers );

            return Stream.concat( readReplicaMembers.stream(), coreMembers.stream() ).filter( not( myself::equals ) ).findFirst();
        }
    }

    private Optional<MemberId> randomCoreMember()
    {
        List<MemberId> coreMembersNotSelf =
                topologyService.localCoreServers().members().keySet().stream().filter( not( myself::equals ) ).collect( Collectors.toList() );
        Collections.shuffle( coreMembersNotSelf );
        if ( coreMembersNotSelf.size() == 0 )
        {
            return Optional.empty();
        }
        return Optional.of( coreMembersNotSelf.get( 0 ) );
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
