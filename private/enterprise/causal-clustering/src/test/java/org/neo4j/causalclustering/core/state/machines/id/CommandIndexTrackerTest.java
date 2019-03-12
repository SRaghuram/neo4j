/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Test;

import org.neo4j.logging.AssertableLogProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CommandIndexTrackerTest
{
    @Test
    public void shouldReflectIncreasingUpdates()
    {
        // given
        CommandIndexTracker commandIndexTracker = new CommandIndexTracker();
        assertEquals( -1, commandIndexTracker.getAppliedCommandIndex(), "Initial command index should equal -1" );
        // when
        commandIndexTracker.registerAppliedCommandIndex( 17 );
        // then
        assertEquals( 17, commandIndexTracker.getAppliedCommandIndex(), "Updated index doesn't match expected value" );
    }

    @Test
    public void shouldIgnoreDecreasingUpdates()
    {
        // given
        CommandIndexTracker commandIndexTracker = new CommandIndexTracker();
        // when
        commandIndexTracker.registerAppliedCommandIndex( 5 );
        commandIndexTracker.registerAppliedCommandIndex( 3 );
        // then
        assertEquals( 5, commandIndexTracker.getAppliedCommandIndex(), "Index should have ignored decreasing update" );
    }
}
