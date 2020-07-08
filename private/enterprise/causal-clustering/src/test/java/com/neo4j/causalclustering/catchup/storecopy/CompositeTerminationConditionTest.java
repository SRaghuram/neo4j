/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.time.FakeClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeTerminationConditionTest
{
    @Test
    void shouldCombineSeveralConditions() throws StoreCopyFailedException
    {
        // given
        var guardA = nonShutdownGuard();
        var guardB = nonShutdownGuard();
        var shutdownConditionA = new IsShutdownTerminationCondition( guardA );
        var shutdownConditionB = new IsShutdownTerminationCondition( guardB );
        var fakeClock = new FakeClock( 0, TimeUnit.SECONDS );
        var maxTime = Duration.ofSeconds( 5 );
        var maxTimeCondition = new MaximumTotalTime( maxTime, fakeClock );
        var compositeCondition = maxTimeCondition.and( shutdownConditionA ).and( shutdownConditionB );

        // when
        compositeCondition.assertContinue(); // nothing happens

        // when
        when( guardA.isShutdown() ).thenReturn( true );

        // then
        assertThrows( StoreCopyFailedException.class, compositeCondition::assertContinue );

        // when
        when( guardA.isShutdown() ).thenReturn( false );

        // then
        compositeCondition.assertContinue(); // Back to passing as guard A is no longer shutdown

        // when
        when( guardB.isShutdown() ).thenReturn( true );

        // then
        assertThrows( StoreCopyFailedException.class, compositeCondition::assertContinue );

        // when
        when( guardB.isShutdown() ).thenReturn( false );
        fakeClock.forward( maxTime.plusSeconds( 1 ) );

        // then
        assertThrows( StoreCopyFailedException.class, compositeCondition::assertContinue ); // guard B is no longer shutdown but time has run out
    }

    @Test
    void shouldSuppressAndThrowMultipleFailures()
    {
        // given
        var guard = nonShutdownGuard();
        var shutdownCondition = new IsShutdownTerminationCondition( guard );
        var fakeClock = new FakeClock( 0, TimeUnit.SECONDS );
        var maxTime = Duration.ofSeconds( 5 );
        var maxTimeCondition = new MaximumTotalTime( maxTime, fakeClock );
        var compositeCondition = shutdownCondition.and( maxTimeCondition );

        // when
        fakeClock.forward( maxTime.plusSeconds( 1 ) );
        when( guard.isShutdown() ).thenReturn( true );

        // then
        var maxTimeException = assertThrows( StoreCopyFailedException.class, maxTimeCondition::assertContinue );
        var shutdownException = assertThrows( StoreCopyFailedException.class, shutdownCondition::assertContinue );
        var compositeException = assertThrows( StoreCopyFailedException.class, compositeCondition::assertContinue );
        assertThat( Exceptions.findCauseOrSuppressed( compositeException, e -> Objects.equals( e.getMessage(), maxTimeException.getMessage() ) ) ).isPresent();
        assertThat( Exceptions.findCauseOrSuppressed( compositeException, e -> Objects.equals( e.getMessage(), shutdownException.getMessage() ) ) ).isPresent();
    }

    private CompositeDatabaseAvailabilityGuard nonShutdownGuard()
    {
        var guard = mock( CompositeDatabaseAvailabilityGuard.class );
        when( guard.isShutdown() ).thenReturn( false );
        return guard;
    }
}
