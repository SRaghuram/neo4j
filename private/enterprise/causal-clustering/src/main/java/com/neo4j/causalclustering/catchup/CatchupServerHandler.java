/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandler;

public interface CatchupServerHandler
{
    ChannelHandler getDatabaseIdRequestHandler( CatchupServerProtocol protocol );

    ChannelHandler txPullRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler storeListingRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler snapshotHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler getAllDatabaseIds( CatchupServerProtocol catchupServerProtocol );
}
