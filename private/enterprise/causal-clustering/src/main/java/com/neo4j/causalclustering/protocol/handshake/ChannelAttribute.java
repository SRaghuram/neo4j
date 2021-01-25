/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import io.netty.util.AttributeKey;

import java.util.concurrent.CompletableFuture;

public class ChannelAttribute
{
    public static final AttributeKey<CompletableFuture<ProtocolStack>> PROTOCOL_STACK = AttributeKey.valueOf( "PROTOCOL_STACK" );
}
