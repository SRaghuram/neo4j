/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.catchup.IncomingResponseValve;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class IncomingResponseValveControllerTest
{
    private final long highWaterMark = 10;
    private final long lowWaterMark = 5;
    private final IncomingResponseValve incomingResponseValve = mock( IncomingResponseValve.class );
    private final IncomingResponseValveController
            incomingResponseValveController = new IncomingResponseValveController( highWaterMark, lowWaterMark, incomingResponseValve );

    @Test
    void shouldStopReadingWhenIncrementedToUpperLimit()
    {
        for ( int i = 0; i < highWaterMark - 1; i++ )
        {
            incomingResponseValveController.increment();
        }
        verify( incomingResponseValve, never() ).shut();
        incomingResponseValveController.increment(); // incremented to upper limit
        verify( incomingResponseValve, times( 1 ) ).shut();
        incomingResponseValveController.increment(); // incremented above upper limit
        verify( incomingResponseValve, times( 1 ) ).shut(); // only trigger once still
    }

    @Test
    void shouldNotStopReadingWhenDecrementedToUpperLimit()
    {
        for ( int i = 0; i < highWaterMark; i++ )
        {
            incomingResponseValveController.increment();
        }
        verify( incomingResponseValve, times( 1 ) ).shut();
        incomingResponseValveController.increment(); // upper limit +1
        incomingResponseValveController.onSuccess(); // decremented to upper limit
        verify( incomingResponseValve, times( 1 ) ).shut(); // only trigger once still
    }

    @Test
    void shouldNotContinueReadingWhenIncrementedToLowerLimit()
    {
        for ( int i = 0; i < lowWaterMark; i++ )
        {
            incomingResponseValveController.increment();
        }
        // incremented to lower watermark...
        verify( incomingResponseValve, never() ).shut(); //... does not trigger
    }

    @Test
    void shouldContinueReadingWhenDecrementedToLowerLimit()
    {
        for ( int i = 0; i < lowWaterMark; i++ )
        {
            incomingResponseValveController.increment();
        }
        incomingResponseValveController.increment(); // incremented above lower limit
        incomingResponseValveController.onSuccess(); // decremented to lower limit ...
        verify( incomingResponseValve, times( 1 ) ).open(); // does trigger
        incomingResponseValveController.onSuccess(); // decremented below lower limit ...
        verify( incomingResponseValve, times( 1 ) ).open(); // does  not trigger
    }

    @Test
    void higherWaterMarkMustBeAboveLower()
    {
        assertThrows( IllegalArgumentException.class, () -> new IncomingResponseValveController( highWaterMark, highWaterMark, incomingResponseValve ) );
        assertThrows( IllegalArgumentException.class, () -> new IncomingResponseValveController( lowWaterMark, highWaterMark, incomingResponseValve ) );
    }
}
