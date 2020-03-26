/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReferenceCounterTest
{
    private ReferenceCounter refCount = new ReferenceCounter();

    @Test
    void shouldHaveValidInitialBehaviour()
    {
        Assertions.assertEquals( 0, refCount.get() );
        Assertions.assertTrue( refCount.tryDispose() );
    }

    @Test
    void shouldNotBeAbleToDisposeWhenActive()
    {
        // when
        refCount.increase();

        // then
        Assertions.assertFalse( refCount.tryDispose() );
    }

    @Test
    void shouldBeAbleToDisposeInactive()
    {
        // given
        refCount.increase();
        refCount.increase();

        // when / then
        refCount.decrease();
        Assertions.assertFalse( refCount.tryDispose() );

        // when / then
        refCount.decrease();
        Assertions.assertTrue( refCount.tryDispose() );
    }

    @Test
    void shouldNotGiveReferenceWhenDisposed()
    {
        // given
        refCount.tryDispose();

        // then
        Assertions.assertFalse( refCount.increase() );
    }

    @Test
    void shouldAdjustCounterWithReferences()
    {
        // when / then
        refCount.increase();
        Assertions.assertEquals( 1, refCount.get() );

        // when / then
        refCount.increase();
        Assertions.assertEquals( 2, refCount.get() );

        // when / then
        refCount.decrease();
        Assertions.assertEquals( 1, refCount.get() );

        // when / then
        refCount.decrease();
        Assertions.assertEquals( 0, refCount.get() );
    }

    @Test
    void shouldThrowIllegalStateExceptionWhenDecreasingPastZero()
    {
        // given
        refCount.increase();
        refCount.decrease();

        // when
        try
        {
            refCount.decrease();
            Assertions.fail();
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }

    @Test
    void shouldThrowIllegalStateExceptionWhenDecreasingOnDisposed()
    {
        // given
        refCount.tryDispose();

        // when
        try
        {
            refCount.decrease();
            Assertions.fail();
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }
}
