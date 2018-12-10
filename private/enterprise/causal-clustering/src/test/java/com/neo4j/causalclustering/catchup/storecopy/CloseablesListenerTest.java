/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertTrue;

public class CloseablesListenerTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldCloseAllReourcesBeforeException() throws Exception
    {
        // given
        CloseablesListener closeablesListener = new CloseablesListener();
        RuntimeException exception = new RuntimeException( "fail" );
        CloseTrackingCloseable kindCloseable1 = new CloseTrackingCloseable();
        CloseTrackingCloseable unkindCloseable = new CloseTrackingCloseable( exception );
        CloseTrackingCloseable kindCloseable2 = new CloseTrackingCloseable();
        closeablesListener.add( kindCloseable1 );
        closeablesListener.add( unkindCloseable );
        closeablesListener.add( kindCloseable2 );

        //then we expect an exception
        expectedException.expect( exception.getClass() );

        // when
        closeablesListener.close();

        //then we expect all have closed
        assertTrue( kindCloseable1.wasClosed );
        assertTrue( unkindCloseable.wasClosed );
        assertTrue( kindCloseable2.wasClosed );
    }

    class CloseTrackingCloseable implements AutoCloseable
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
