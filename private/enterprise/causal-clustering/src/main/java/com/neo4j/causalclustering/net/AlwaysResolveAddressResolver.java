/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultNameResolver;
import io.netty.resolver.InetSocketAddressResolver;
import io.netty.util.concurrent.EventExecutor;

import java.net.InetSocketAddress;

final class AlwaysResolveAddressResolver extends InetSocketAddressResolver
{
    static AddressResolverGroup<InetSocketAddress> INSTANCE = new AddressResolverGroup<>()
    {
        @Override
        protected AddressResolver<InetSocketAddress> newResolver( EventExecutor executor )
        {
            return new AlwaysResolveAddressResolver( executor );
        }
    };

    private AlwaysResolveAddressResolver( EventExecutor executor )
    {
        super( executor, new DefaultNameResolver( executor ) );
    }

    @Override
    protected boolean doIsResolved( InetSocketAddress address )
    {
        return false;
    }
}
