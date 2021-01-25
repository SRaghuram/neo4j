/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IsShutdownTerminationConditionTest
{
    @Test
    void shouldThrowIfAvailabilityGuardIsShutdown() throws StoreCopyFailedException
    {
        // given
        var guard = mock( CompositeDatabaseAvailabilityGuard.class );
        var isShutdownCondition = new IsShutdownTerminationCondition( guard );
        when( guard.isShutdown() ).thenReturn( false ).thenReturn( true );

        // when/then
        isShutdownCondition.assertContinue(); // nothing happens
        assertThrows( StoreCopyFailedException.class, isShutdownCondition::assertContinue );
    }
}
