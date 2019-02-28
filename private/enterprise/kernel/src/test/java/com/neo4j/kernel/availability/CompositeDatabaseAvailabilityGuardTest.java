/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.availability;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.time.Clock;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DescriptiveAvailabilityRequirement;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.logging.internal.NullLogService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class CompositeDatabaseAvailabilityGuardTest
{

    private final DescriptiveAvailabilityRequirement requirement = new DescriptiveAvailabilityRequirement( "testRequirement" );
    private CompositeDatabaseAvailabilityGuard compositeGuard;
    private DatabaseAvailabilityGuard defaultGuard;
    private DatabaseAvailabilityGuard systemGuard;
    private Clock mockClock;

    @BeforeEach
    void setUp()
    {
        mockClock = mock( Clock.class );
        compositeGuard = new CompositeDatabaseAvailabilityGuard( mockClock, NullLogService.getInstance() );
        defaultGuard = compositeGuard.createDatabaseAvailabilityGuard( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        systemGuard = compositeGuard.createDatabaseAvailabilityGuard( SYSTEM_DATABASE_NAME );
    }

    @Test
    void availabilityRequirementOnMultipleGuards()
    {
        assertTrue( defaultGuard.isAvailable() );
        assertTrue( systemGuard.isAvailable() );

        compositeGuard.require( new DescriptiveAvailabilityRequirement( "testRequirement" ) );

        assertFalse( defaultGuard.isAvailable() );
        assertFalse( systemGuard.isAvailable() );
    }

    @Test
    void availabilityFulfillmentOnMultipleGuards()
    {
        compositeGuard.require( requirement );

        assertFalse( defaultGuard.isAvailable() );
        assertFalse( systemGuard.isAvailable() );

        compositeGuard.fulfill( requirement );

        assertTrue( defaultGuard.isAvailable() );
        assertTrue( systemGuard.isAvailable() );
    }

    @Test
    void availableWhenAllGuardsAreAvailable()
    {
        assertTrue( compositeGuard.isAvailable() );

        defaultGuard.require( requirement );

        assertFalse( compositeGuard.isAvailable() );
    }

    @Test
    void shutdownWhenAnyOfTheGuardsAreShuttingDown()
    {
        assertFalse( compositeGuard.isShutdown() );

        defaultGuard.shutdown();

        assertTrue( compositeGuard.isShutdown() );
    }

    @Test
    void addRemoveListenersOnAllGuards()
    {
        AvailabilityListener listener = mock( AvailabilityListener.class );
        compositeGuard.addListener( listener );

        compositeGuard.require( requirement );

        verify( listener, times( 2 ) ).unavailable();

        compositeGuard.fulfill( requirement );

        verify( listener, times( 2 ) ).available();

        compositeGuard.removeListener( listener );

        compositeGuard.require( requirement );
        compositeGuard.fulfill( requirement );

        verify( listener, times( 2 ) ).unavailable();
        verify( listener, times( 2 ) ).available();
    }

    @Test
    void availabilityTimeoutSharedAcrossAllGuards()
    {
        compositeGuard.require( requirement );
        MutableLong counter = new MutableLong(  );

        when( mockClock.millis() ).thenAnswer( (Answer<Long>) invocation ->
        {
            if ( counter.longValue() == 7 )
            {
                defaultGuard.fulfill( requirement );
            }
            return counter.incrementAndGet();
        } );

        assertFalse( compositeGuard.isAvailable( 10 ) );

        assertThat( counter.getValue(), lessThan( 20L ) );
        assertTrue( defaultGuard.isAvailable() );
        assertFalse( systemGuard.isAvailable() );
    }

    @Test
    void awaitCheckTimeoutSharedAcrossAllGuards()
    {
        compositeGuard.require( requirement );
        MutableLong counter = new MutableLong(  );

        when( mockClock.millis() ).thenAnswer( (Answer<Long>) invocation ->
        {
            if ( counter.longValue() == 7 )
            {
                defaultGuard.fulfill( requirement );
            }
            return counter.incrementAndGet();
        } );

        assertThrows( UnavailableException.class, () -> compositeGuard.await( 10 ) );

        assertThat( counter.getValue(), lessThan( 20L ) );
        assertTrue( defaultGuard.isAvailable() );
        assertFalse( systemGuard.isAvailable() );
    }
}
