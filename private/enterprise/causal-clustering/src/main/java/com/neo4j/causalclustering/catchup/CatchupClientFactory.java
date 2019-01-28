/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import io.netty.channel.socket.SocketChannel;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Function;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class CatchupClientFactory extends LifecycleAdapter
{
    private final Log log;
    private final CatchupChannelPool pool;
    private final String defaultDatabaseName;
    private final Duration inactivityTimeout;

    public CatchupClientFactory( LogProvider logProvider, Clock clock, Function<CatchupResponseHandler,HandshakeClientInitializer> channelInitializerFactory,
            String defaultDatabaseName, Duration inactivityTimeout, JobScheduler scheduler,
            BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration )
    {
        this.log = logProvider.getLog( getClass() );
        this.defaultDatabaseName = defaultDatabaseName;
        this.inactivityTimeout = inactivityTimeout;
        this.pool = new CatchupChannelPool( bootstrapConfiguration, scheduler, clock, channelInitializerFactory );
    }

    public VersionedCatchupClients getClient( AdvertisedSocketAddress upstream, Log log )
    {
        return new CatchupClient( pool.acquire( upstream ), defaultDatabaseName, inactivityTimeout, log );
    }

    @Override
    public void start()
    {
        pool.start();
    }

    @Override
    public void stop()
    {
        log.info( "CatchUpClient stopping" );
        pool.stop();
    }
}
