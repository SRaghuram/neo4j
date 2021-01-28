/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.catchup.FlowControl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class BackpressureTest
{
    private final long highWaterMark = 10;
    private final long lowWaterMark = 5;
    private final FlowControl flowControl = mock( FlowControl.class );
    private final Backpressure backpressure = new Backpressure( highWaterMark, lowWaterMark, flowControl );

    @Test
    void shouldStopReadingWhenIncrementedToHigherWatermark()
    {
        for ( int i = 0; i < highWaterMark - 1; i++ )
        {
            backpressure.scheduledJob();
        }
        verify( flowControl, never() ).stopReading();
        backpressure.scheduledJob(); // incremented to higher watermark
        verify( flowControl, times( 1 ) ).stopReading();
        backpressure.scheduledJob(); // incremented above higher watermark
        verify( flowControl, times( 1 ) ).stopReading(); // only trigger once still
    }

    @Test
    void shouldNotStopReadingWhenDecrementedToHigherWatermark()
    {
        for ( int i = 0; i < highWaterMark; i++ )
        {
            backpressure.scheduledJob();
        }
        verify( flowControl, times( 1 ) ).stopReading();
        backpressure.scheduledJob(); // high watermark +1
        backpressure.onSuccess(); // decremented to higher watermark
        verify( flowControl, times( 1 ) ).stopReading(); // only trigger once still
    }

    @Test
    void shouldNotContinueReadingWhenIncrementedToLowerWatermark()
    {
        for ( int i = 0; i < lowWaterMark; i++ )
        {
            backpressure.scheduledJob();
        }
        // incremented to lower watermark...
        verify( flowControl, never() ).stopReading(); //... does not trigger
    }

    @Test
    void shouldContinueReadingWhenDecrementedToLowerWatermark()
    {
        for ( int i = 0; i < lowWaterMark; i++ )
        {
            backpressure.scheduledJob();
        }
        backpressure.scheduledJob(); // incremented above lower watermark
        backpressure.onSuccess(); // decremented to lower watermark ...
        verify( flowControl, times( 1 ) ).continueReading(); // does trigger
        backpressure.onSuccess(); // decremented below lower watermark ...
        verify( flowControl, times( 1 ) ).continueReading(); // does  not trigger
    }

    @Test
    void higherWaterMarkMustBeAboveLower()
    {
        assertThrows( IllegalArgumentException.class, () -> new Backpressure( highWaterMark, highWaterMark, flowControl ) );
        assertThrows( IllegalArgumentException.class, () -> new Backpressure( lowWaterMark, highWaterMark, flowControl ) );
    }
}
