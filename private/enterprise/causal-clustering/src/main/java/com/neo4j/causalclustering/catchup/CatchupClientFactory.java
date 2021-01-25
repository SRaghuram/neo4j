/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import java.time.Duration;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

public class CatchupClientFactory implements Lifecycle
{
    private final CatchupChannelPoolService pool;
    private final Duration inactivityTimeout;

    public CatchupClientFactory( Duration inactivityTimeout, CatchupChannelPoolService catchupChannelPoolService )
    {
        this.inactivityTimeout = inactivityTimeout;
        this.pool = catchupChannelPoolService;
    }

    public VersionedCatchupClients getClient( SocketAddress upstream, Log log )
    {
        return new CatchupClient( pool.acquire( upstream ).thenApply( CatchupChannel::new ), inactivityTimeout, log );
    }

    @Override
    public void init()
    {
        pool.init();
    }

    @Override
    public void start()
    {
        pool.start();
    }

    @Override
    public void stop()
    {
        pool.stop();
    }

    @Override
    public void shutdown()
    {
        pool.shutdown();
    }
}
