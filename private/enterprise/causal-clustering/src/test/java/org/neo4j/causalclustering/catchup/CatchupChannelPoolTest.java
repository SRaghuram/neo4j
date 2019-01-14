/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import org.junit.Test;

import java.net.ConnectException;
import java.util.function.Function;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CatchupChannelPoolTest
{
    @Test
    public void shouldReUseAChannelThatWasReleased() throws Exception
    {
        // given
        CatchupChannelPool<TestChannel> pool = new CatchupChannelPool<>( TestChannel::new );

        // when
        TestChannel channelA = pool.acquire( localAddress( 1 ) );
        pool.release( channelA );
        TestChannel channelB = pool.acquire( localAddress( 1 ) );

        // then
        assertSame( channelA, channelB );
    }

    @Test
    public void shouldCreateANewChannelIfFirstChannelIsDisposed() throws Exception
    {
        // given
        CatchupChannelPool<TestChannel> pool = new CatchupChannelPool<>( TestChannel::new );

        // when
        TestChannel channelA = pool.acquire( localAddress( 1 ) );
        pool.dispose( channelA );
        TestChannel channelB = pool.acquire( localAddress( 1 ) );

        // then
        assertNotSame( channelA, channelB );
    }

    @Test
    public void shouldCreateANewChannelIfFirstChannelIsStillActive() throws Exception
    {
        // given
        CatchupChannelPool<TestChannel> pool = new CatchupChannelPool<>( TestChannel::new );

        // when
        TestChannel channelA = pool.acquire( localAddress( 1 ) );
        TestChannel channelB = pool.acquire( localAddress( 1 ) );

        // then
        assertNotSame( channelA, channelB );
    }

    @Test
    public void shouldCleanUpOnClose() throws Exception
    {
        // given
        CatchupChannelPool<TestChannel> pool = new CatchupChannelPool<>( TestChannel::new );

        TestChannel channelA = pool.acquire( localAddress( 1 ) );
        TestChannel channelB = pool.acquire( localAddress( 1 ) );
        TestChannel channelC = pool.acquire( localAddress( 1 ) );

        pool.release( channelA );
        pool.release( channelC );

        TestChannel channelD = pool.acquire( localAddress( 2 ) );
        TestChannel channelE = pool.acquire( localAddress( 2 ) );
        TestChannel channelF = pool.acquire( localAddress( 2 ) );

        // when
        pool.close();

        // then
        assertTrue( channelA.closed );
        assertTrue( channelB.closed );
        assertTrue( channelC.closed );
        assertTrue( channelD.closed );
        assertTrue( channelE.closed );
        assertTrue( channelF.closed );
    }

    @Test
    public void shouldFailWithExceptionIsChannelIsNotActive()
    {
        CatchupChannelPool<TestChannel> pool = new CatchupChannelPool<>( advertisedSocketAddress -> new TestChannel( advertisedSocketAddress, false ) );

        try
        {
            pool.acquire( localAddress( 1 ) );
        }
        catch ( Exception e )
        {
            assertEquals( ConnectException.class, e.getClass() );
            assertEquals( "Unable to connect to localhost:1", e.getMessage() );
            return;
        }
        fail();
    }

    @Test
    public void shouldCheckConnectionOnIdleChannelFirst()
    {
        // given
        CatchupChannelPool<TestChannel> pool = new CatchupChannelPool<>( new Function<AdvertisedSocketAddress,TestChannel>()
        {
            boolean firstIsActive = true;

            @Override
            public TestChannel apply( AdvertisedSocketAddress address )
            {
                TestChannel testChannel = new TestChannel( address, firstIsActive );
                firstIsActive = false;
                return testChannel;
            }
        } );

        TestChannel channel = null;
        try
        {
            channel = pool.acquire( localAddress( 1 ) );
            assertNotNull( channel );
        }
        catch ( Exception e )
        {
            fail( "Not expected exception" );
        }

        // when channel loses connection in idle
        channel.isActive = false;
        pool.release( channel );

        try
        {
            // then
            pool.acquire( localAddress( 1 ) );
        }
        catch ( Exception e )
        {
            assertEquals( ConnectException.class, e.getClass() );
            assertEquals( "Unable to connect to localhost:1", e.getMessage() );
            return;
        }
        fail();
    }

    private static class TestChannel implements CatchupChannelPool.Channel
    {
        private final AdvertisedSocketAddress address;
        private boolean isActive;
        private boolean closed;

        TestChannel( AdvertisedSocketAddress address, boolean isActive )
        {

            this.address = address;
            this.isActive = isActive;
        }

        TestChannel( AdvertisedSocketAddress address )
        {
            this( address, true );
        }

        @Override
        public AdvertisedSocketAddress destination()
        {
            return address;
        }

        @Override
        public void connect()
        {
            // do nothing
        }

        @Override
        public boolean isActive()
        {
            return isActive;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static AdvertisedSocketAddress localAddress( int port )
    {
        return new AdvertisedSocketAddress( "localhost", port );
    }
}
