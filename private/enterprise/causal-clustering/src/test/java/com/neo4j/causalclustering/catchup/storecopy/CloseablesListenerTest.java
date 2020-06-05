/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CloseablesListenerTest
{
    @Test
    void shouldCloseAllResourcesBeforeException() throws Exception
    {
        // given
        var closeablesListener = new CloseablesListener();
        var exception = new RuntimeException( "fail" );
        var kindCloseable1 = new CloseTrackingCloseable();
        var unkindCloseable = new CloseTrackingCloseable( exception );
        var kindCloseable2 = new CloseTrackingCloseable();
        closeablesListener.add( kindCloseable1 );
        closeablesListener.add( unkindCloseable );
        closeablesListener.add( kindCloseable2 );

        // when / then we expect an exception
        var thrownException = assertThrows( exception.getClass(), closeablesListener::close );
        assertEquals( exception, thrownException.getCause() );

        //then we expect all have closed
        assertTrue( kindCloseable1.wasClosed );
        assertTrue( unkindCloseable.wasClosed );
        assertTrue( kindCloseable2.wasClosed );
    }

    static class CloseTrackingCloseable implements AutoCloseable
    {
        private final Exception throwOnClose;

        private CloseTrackingCloseable()
        {
            this( null );
        }

        CloseTrackingCloseable( Exception throwOnClose )
        {
            this.throwOnClose = throwOnClose;
        }

        boolean wasClosed;

        @Override
        public void close() throws Exception
        {
            wasClosed = true;
            if ( throwOnClose != null )
            {
                throw throwOnClose;
            }
        }
    }
}
