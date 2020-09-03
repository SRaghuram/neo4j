/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.identity.ServerId;
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
    public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId )
    {
        if ( counter.shouldReturnOnlyCores() )
        {
            return randomCoreServer( namedDatabaseId );
        }
        else
        {
            // shuffled servers
            List<ServerId> readReplicaServers = new ArrayList<>( topologyService.readReplicaTopologyForDatabase( namedDatabaseId ).servers().keySet() );
            Collections.shuffle( readReplicaServers );

            List<ServerId> coreServers = new ArrayList<>( topologyService.coreTopologyForDatabase( namedDatabaseId ).servers().keySet() );
            Collections.shuffle( coreServers );

            return Stream.concat( readReplicaServers.stream(), coreServers.stream() ).filter( not( myself::equals ) ).findFirst();
        }
    }

    @Override
    public Collection<ServerId> upstreamServersForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var cores = otherCoreServers( namedDatabaseId );
        var readReplicas = otherReadReplicas( namedDatabaseId );
        if ( counter.shouldReturnOnlyCores() || readReplicas.isEmpty() )
        {
            Collections.shuffle( cores );
            return cores;
        }
        else
        {
            Collections.shuffle( readReplicas );
            return readReplicas;
        }
    }

    private Optional<ServerId> randomCoreServer( NamedDatabaseId namedDatabaseId )
    {
        List<ServerId> coreMembersNotSelf = topologyService.coreTopologyForDatabase( namedDatabaseId )
                .servers().keySet().stream()
                .filter( not( myself::equals ) )
                .collect( Collectors.toList() );

        if ( coreMembersNotSelf.isEmpty() )
        {
            return Optional.empty();
        }

        int randomIndex = ThreadLocalRandom.current().nextInt( coreMembersNotSelf.size() );
        return Optional.of( coreMembersNotSelf.get( randomIndex ) );
    }

    private List<ServerId> otherCoreServers( NamedDatabaseId namedDatabaseId )
    {
        return topologyService.coreTopologyForDatabase( namedDatabaseId )
                .servers().keySet().stream()
                .filter( not( myself::equals ) )
                .collect( Collectors.toList() );
    }

    private List<ServerId> otherReadReplicas( NamedDatabaseId namedDatabaseId )
    {
        return topologyService.readReplicaTopologyForDatabase( namedDatabaseId )
                .servers().keySet().stream()
                .filter( not( myself::equals ) )
                .collect( Collectors.toList() );
    }

    private static class ModuloCounter
    {
        private final int modulo;
        private int counter;

        ModuloCounter( int modulo )
        {
            this.modulo = modulo;
        }

        boolean shouldReturnOnlyCores()
        {
            counter = (counter + 1) % modulo;
            return counter == 0;
        }
    }
}
