/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.DefaultTimeoutStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.time.Clocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AddressRepositoryTest
{
    final NamedDatabaseId databaseId = DatabaseIdFactory.from( "dbID", UUID.randomUUID() );
    CatchupAddressProvider addressProvider;
    TimeoutStrategy timeoutStrategy = new DefaultTimeoutStrategy( 100, 1000, TimeUnit.MILLISECONDS, i -> i + 100 );

    @BeforeEach
    void setUp()
    {
        addressProvider = mock( CatchupAddressProvider.class );
    }

    @Test
    void shouldGetFirstFreeAddress() throws CatchupAddressResolutionException
    {

        final var socketAddresses = List.of( new SocketAddress( "a" ) );
        when( addressProvider.allSecondaries( databaseId ) ).thenReturn( socketAddresses );
        final var repository = new AddressRepository( addressProvider, databaseId, Clocks.nanoClock(), timeoutStrategy, mock( Log.class ) );

        //when try to get address that exits
        assertEquals( Optional.of( socketAddresses.get( 0 ) ), repository.nextFreeAddress() );

        // when try to get a next address and there are no more addresses left
        assertEquals( Optional.empty(), repository.nextFreeAddress() );
    }

    @Test
    void addressShouldBeAvailableWhenItIsReleasedSuccessfully() throws CatchupAddressResolutionException
    {
        final var socketAddresses = List.of( new SocketAddress( "a" ) );
        when( addressProvider.allSecondaries( databaseId ) ).thenReturn( socketAddresses );
        final var repository = new AddressRepository( addressProvider, databaseId, Clocks.nanoClock(), timeoutStrategy, mock( Log.class ) );

        //when try to get address that exits
        assertEquals( Optional.of( socketAddresses.get( 0 ) ), repository.nextFreeAddress() );

        //then release address
        repository.release( socketAddresses.get( 0 ) );

        // when try to get a next address and there are no more addresses left
        assertTrue( repository.nextFreeAddress().isPresent() );
    }

    @Test
    void addressThatIsNotPartOfInitialListIsIgnored() throws CatchupAddressResolutionException
    {
        List<SocketAddress> socketAddresses = Collections.emptyList();
        when( addressProvider.allSecondaries( databaseId ) ).thenReturn( socketAddresses );
        final var repository = new AddressRepository( addressProvider, databaseId, Clocks.nanoClock(), timeoutStrategy, mock( Log.class ) );

        //when
        assertEquals( Optional.empty(), repository.nextFreeAddress() );
        repository.release( new SocketAddress( "b" ) );

        //then
        assertEquals( Optional.empty(), repository.nextFreeAddress() );
    }

    @Test
    void addressShouldBeAvailableWhenPenaltyIsRemoved() throws CatchupAddressResolutionException
    {
        final var fakeClock = Clocks.fakeClock();
        final var socketAddresses = List.of( new SocketAddress( "a" ) );
        when( addressProvider.allSecondaries( databaseId ) ).thenReturn( socketAddresses );
        final var repository = new AddressRepository( addressProvider, databaseId, fakeClock, timeoutStrategy, mock( Log.class ) );

        //when address is penalised
        final var address = repository.nextFreeAddress().get();
        repository.releaseAndPenalise( address );

        //then address is unavailable
        assertTrue( repository.nextFreeAddress().isEmpty() );

        //when penalty is lifted from the address
        fakeClock.forward( 300, TimeUnit.MILLISECONDS );

        //then address is available
        assertEquals( Optional.of( socketAddresses.get( 0 ) ), repository.nextFreeAddress() );
    }

    @Test
    void twoSequencePenaltiesShouldIncreaseThePenaltyTimeout() throws CatchupAddressResolutionException
    {
        final var fakeClock = Clocks.fakeClock();
        final var socketAddresses = List.of( new SocketAddress( "a" ) );
        when( addressProvider.allSecondaries( databaseId ) ).thenReturn( socketAddresses );
        final var repository = new AddressRepository( addressProvider, databaseId, fakeClock, timeoutStrategy, mock( Log.class ) );

        //when address is penalised for the first time
        final var address = repository.nextFreeAddress().get();
        repository.releaseAndPenalise( address );

        //then clock is moved forward
        fakeClock.forward( Duration.ofMillis( 110 ) );
        assertEquals( Optional.of( socketAddresses.get( 0 ) ), repository.nextFreeAddress() );

        //then address is address is penalized second time, penalised time should be 200
        repository.releaseAndPenalise( address );

        //then clock is moved forward
        fakeClock.forward( Duration.ofMillis( 150 ) );
        assertTrue( repository.nextFreeAddress().isEmpty() );

        //then clock is moved forward
        fakeClock.forward( Duration.ofMillis( 60 ) );
        assertTrue( repository.nextFreeAddress().isPresent() );
    }

    @Test
    void sequenceOfPenaliseAndReleaseShouldnIncreaseThePenaltyTimeout() throws CatchupAddressResolutionException
    {
        final var fakeClock = Clocks.fakeClock();
        final var socketAddresses = List.of( new SocketAddress( "a" ) );
        when( addressProvider.allSecondaries( databaseId ) ).thenReturn( socketAddresses );
        final var repository = new AddressRepository( addressProvider, databaseId, fakeClock, timeoutStrategy, mock( Log.class ) );

        //when address is penalised for the first time
        final var address = repository.nextFreeAddress().get();
        repository.releaseAndPenalise( address );

        //then clock is moved forward
        fakeClock.forward( Duration.ofMillis( 110 ) );
        assertEquals( Optional.of( socketAddresses.get( 0 ) ), repository.nextFreeAddress() );

        //then address is release successfully
        repository.release( address );

        //then get next free address
        assertEquals( Optional.of( socketAddresses.get( 0 ) ), repository.nextFreeAddress() );
        repository.releaseAndPenalise( address );

        //then clock penalty timeout shouldn't be increased
        fakeClock.forward( Duration.ofMillis( 110 ) );
        assertEquals( Optional.of( socketAddresses.get( 0 ) ), repository.nextFreeAddress() );

    }
}
