/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;

import org.neo4j.test.ports.PortAuthority;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlwaysResolveAddressResolverTest
{
    @Test
    void shouldAlwaysResolveAddress() throws InterruptedException
    {
        var port = PortAuthority.allocatePort();
        var address = new InetSocketAddress( port );

        assertFalse( address.isUnresolved() );

        var group = new NioEventLoopGroup();

        var trackingResolverGroup = new TrackingResolverGroup();

        try
        {
            var connect = new Bootstrap()
                    .group( group )
                    .channel( NioSocketChannel.class )
                    .resolver( trackingResolverGroup )
                    .handler( new ChannelInboundHandlerAdapter() )
                    .connect( address );

            // not expected to connect
            assertThrows( Exception.class, connect::sync );

            assertTrue( trackingResolverGroup.tracker.checkResolved > 0 );
            assertEquals( 1, trackingResolverGroup.tracker.resolve );
            assertEquals( 0, trackingResolverGroup.tracker.resolveAll );
        }
        finally
        {
            group.shutdownGracefully().sync();
        }
    }

    private static class TrackingResolverGroup extends AddressResolverGroup<InetSocketAddress>
    {

        private TrackingAddressResolver tracker;

        @Override
        protected AddressResolver<InetSocketAddress> newResolver( EventExecutor executor )
        {
            tracker = new TrackingAddressResolver( executor );
            return tracker;
        }
    }

    private static class TrackingAddressResolver extends AlwaysResolveAddressResolver
    {
        int resolve;
        int resolveAll;
        int checkResolved;

        TrackingAddressResolver( EventExecutor executor )
        {
            super( executor );
        }

        @Override
        protected boolean doIsResolved( InetSocketAddress address )
        {
            checkResolved++;
            return super.doIsResolved( address );
        }

        @Override
        protected void doResolve( InetSocketAddress unresolvedAddress, Promise<InetSocketAddress> promise ) throws Exception
        {
            resolve++;
            super.doResolve( unresolvedAddress, promise );
        }

        @Override
        protected void doResolveAll( InetSocketAddress unresolvedAddress, Promise<List<InetSocketAddress>> promise ) throws Exception
        {
            resolveAll++;
            super.doResolveAll( unresolvedAddress, promise );
        }
    }
}
