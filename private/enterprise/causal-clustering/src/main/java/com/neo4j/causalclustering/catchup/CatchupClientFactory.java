/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import java.time.Duration;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class CatchupClientFactory
{
    private final CatchupChannelPool pool;
    private final String defaultDatabaseName;
    private final Duration inactivityTimeout;

    public CatchupClientFactory( String defaultDatabaseName, Duration inactivityTimeout, CatchupChannelPool catchupChannelPool )
    {
        this.defaultDatabaseName = defaultDatabaseName;
        this.inactivityTimeout = inactivityTimeout;
        this.pool = catchupChannelPool;
    }

    public VersionedCatchupClients getClient( AdvertisedSocketAddress upstream, Log log )
    {
        return new CatchupClient( pool.acquire( upstream ), defaultDatabaseName, inactivityTimeout, log );
    }
}
