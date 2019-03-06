/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Test;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CommandIndexTrackerTest
{

    @Test
    public void shouldReflectIncreasingUpdates()
    {
        // given
        CommandIndexTracker commandIndexTracker = new CommandIndexTracker( NullLogProvider.getInstance() );
        assertEquals( -1, commandIndexTracker.getAppliedCommandIndex(), "Initial command index should equal -1" );
        // when
        commandIndexTracker.setAppliedCommandIndex( 17 );
        // then
        assertEquals( 17, commandIndexTracker.getAppliedCommandIndex(), "Updated index doesn't match expected value" );
    }

    @Test
    public void shouldLogAndIgnoreDecreasingUpdates()
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        CommandIndexTracker commandIndexTracker = new CommandIndexTracker( logProvider );
        // when
        commandIndexTracker.setAppliedCommandIndex( 5 );
        commandIndexTracker.setAppliedCommandIndex( 3 );
        // then
        assertEquals( 5, commandIndexTracker.getAppliedCommandIndex(), "Index should have ignored decreasing update" );
        logProvider.assertContainsLogCallContaining( "Warning, a command index tracker may only increase!" );
    }
}
