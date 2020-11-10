/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;

import static java.util.function.Predicate.not;
import static org.neo4j.internal.helpers.TimeoutStrategy.Timeout;

class AddressRepository
{
    private final Log log;
    private final NamedDatabaseId namedDatabaseId;
    private final CatchupAddressProvider addressProvider;
    private final Map<SocketAddress,AddressState> unavailableAddresses = new HashMap<>();
    private final Map<SocketAddress,Timeout> addressNextPenalty = new HashMap<>();
    private final TimeoutStrategy timeoutStrategy;
    private final Clock clock;

    AddressRepository( CatchupAddressProvider addressProvider, NamedDatabaseId namedDatabaseId, Clock clock, TimeoutStrategy timeoutStrategy, Log log )
    {
        this.log = log;
        this.namedDatabaseId = namedDatabaseId;
        this.addressProvider = addressProvider;
        this.timeoutStrategy = timeoutStrategy;
        this.clock = clock;
    }

    public synchronized Optional<SocketAddress> nextFreeAddress()
    {
        final var nextAddresses = getSocketAddresses();
        cleanAvailableAddresses();

        return nextAddresses.stream().filter( not( unavailableAddresses::containsKey ) )
                            .peek( address ->
                                   {
                                       final var timeout = addressNextPenalty.computeIfAbsent( address, a -> timeoutStrategy.newTimeout() );
                                       final var penaltyTimeout = Duration.ofMillis( timeout.getMillis() );
                                       unavailableAddresses.put( address, new AddressState( clock, penaltyTimeout ) );
                                   } )
                            .findFirst();
    }

    public synchronized void release( SocketAddress socketAddress )
    {
        final var context = unavailableAddresses.get( socketAddress );
        if ( context != null )
        {
            context.release();
            addressNextPenalty.remove( socketAddress );
        }
    }

    public synchronized void releaseAndPenalise( SocketAddress socketAddress )
    {
        final var context = unavailableAddresses.get( socketAddress );
        if ( context != null )
        {
            context.release();

            context.penalise();
            addressNextPenalty.get( socketAddress ).increment();
        }
    }

    private Set<SocketAddress> getSocketAddresses()
    {
        try
        {
            return new HashSet<>( addressProvider.allSecondaries( namedDatabaseId ) );
        }
        catch ( CatchupAddressResolutionException e )
        {
            log.warn( "Unable to resolve address for StoreCopyRequest. %s", e.getMessage() );
        }
        return Set.of();
    }

    private void cleanAvailableAddresses()
    {
        unavailableAddresses.entrySet().removeIf( entry -> entry.getValue().isAvailable() );
    }

    private static class AddressState
    {
        private Instant penaltyExpiration = Instant.MIN;
        private boolean acquired = true;
        private final Duration penaltyTimeout;
        private final Clock clock;

        private AddressState( Clock clock, Duration penaltyTimeout )
        {
            this.clock = clock;
            this.penaltyTimeout = penaltyTimeout;
        }

        public boolean isAvailable()
        {
            return !acquired && !isPenalised();
        }

        public void release()
        {
            acquired = false;
        }

        public void penalise()
        {
            penaltyExpiration = clock.instant().plus( penaltyTimeout );
        }

        private boolean isPenalised()
        {
            return clock.instant().isBefore( penaltyExpiration );
        }
    }
}

