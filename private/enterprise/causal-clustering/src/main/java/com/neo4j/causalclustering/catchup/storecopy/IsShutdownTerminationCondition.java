/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.neo4j.kernel.availability.AvailabilityGuard;

public class IsShutdownTerminationCondition implements TerminationCondition
{
    private final AvailabilityGuard availabilityGuard;

    IsShutdownTerminationCondition( AvailabilityGuard availabilityGuard )
    {
        this.availabilityGuard = availabilityGuard;
    }

    @Override
    public void assertContinue() throws StoreCopyFailedException
    {
        if ( availabilityGuard.isShutdown() )
        {
            throw new StoreCopyFailedException( "Database is shutdown!" );
        }
    }
}
