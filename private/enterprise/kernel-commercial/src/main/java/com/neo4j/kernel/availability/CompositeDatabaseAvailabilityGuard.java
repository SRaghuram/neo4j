/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.availability;

import java.time.Clock;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.logging.LogService;

import static java.util.stream.Collectors.joining;

/**
 * Composite availability guard that makes decision about its availability based on multiple underlying database specific availability guards.
 * Any fulfillment, require, available, etc requests will be redistributed to all underlying availability guards.
 *
 * @see AvailabilityGuard
 */
public class CompositeDatabaseAvailabilityGuard implements AvailabilityGuard
{
    private final Clock clock;
    private final LogService logService;
    private final CopyOnWriteArrayList<DatabaseAvailabilityGuard> guards = new CopyOnWriteArrayList<>();

    public CompositeDatabaseAvailabilityGuard( Clock clock, LogService logService )
    {
        this.clock = clock;
        this.logService = logService;
    }

    public DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( String databaseName )
    {
        DatabaseAvailabilityGuard guard = new DatabaseAvailabilityGuard( databaseName, clock, logService.getInternalLog( DatabaseAvailabilityGuard.class ) );
        guards.add( guard );
        return guard;
    }

    @Override
    public void require( AvailabilityRequirement requirement )
    {
        guards.forEach( guard -> guard.require( requirement ) );
    }

    @Override
    public void fulfill( AvailabilityRequirement requirement )
    {
        guards.forEach( guard -> guard.fulfill( requirement ) );
    }

    @Override
    public boolean isAvailable()
    {
        return guards.stream().allMatch( DatabaseAvailabilityGuard::isAvailable );
    }

    /**
     * Since lifecycle of databases in 3.5 is glued together we can assume that we shutdown databases together.
     */
    @Override
    public boolean isShutdown()
    {
        return guards.stream().anyMatch( DatabaseAvailabilityGuard::isShutdown );
    }

    @Override
    public boolean isAvailable( long millis )
    {
        long totalWait = 0;
        for ( DatabaseAvailabilityGuard guard : guards )
        {
            long startMillis = clock.millis();
            if ( !guard.isAvailable( Math.max( 0, millis - totalWait ) ) )
            {
                return false;
            }
            totalWait += clock.millis() - startMillis;
            if ( totalWait > millis )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void checkAvailable() throws UnavailableException
    {
        for ( DatabaseAvailabilityGuard guard : guards )
        {
            guard.checkAvailable();
        }
    }

    @Override
    public void await( long millis ) throws UnavailableException
    {
        long totalWait = 0;
        for ( DatabaseAvailabilityGuard guard : guards )
        {
            long startMillis = clock.millis();
            guard.await( Math.max( 0, millis - totalWait ) );
            totalWait += clock.millis() - startMillis;
            if ( totalWait > millis )
            {
                throw new UnavailableException( "Database is not available: " + describeGuards() );
            }
        }
    }

    @Override
    public void addListener( AvailabilityListener listener )
    {
        guards.forEach( guard -> guard.addListener( listener ) );
    }

    @Override
    public void removeListener( AvailabilityListener listener )
    {
        guards.forEach( guard -> guard.removeListener( listener ) );
    }

    private String describeGuards()
    {
        return guards.stream().map( DatabaseAvailabilityGuard::describeWhoIsBlocking ).collect( joining( ", " ) );
    }
}
