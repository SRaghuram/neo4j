/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandler;

import java.util.Optional;

public interface CatchupServerHandler
{
    ChannelHandler txPullRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler storeListingRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler getIndexSnapshotRequestHandler( CatchupServerProtocol catchupServerProtocol );

    Optional<ChannelHandler> snapshotHandler( CatchupServerProtocol catchupServerProtocol );
}
