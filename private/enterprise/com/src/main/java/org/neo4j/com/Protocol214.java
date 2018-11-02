/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

import java.nio.ByteBuffer;

import org.neo4j.storageengine.api.StoreId;

public class Protocol214 extends Protocol
{
    public Protocol214( int chunkSize, byte applicationProtocolVersion, byte internalProtocolVersion )
    {
        super( chunkSize, applicationProtocolVersion, internalProtocolVersion );
    }

    @Override
    protected StoreId readStoreId( ChannelBuffer source, ByteBuffer byteBuffer )
    {
        byteBuffer.clear();
        byteBuffer.limit( 8 + 8 + 8 + 8 + 8 ); // creation time, random id, store version, upgrade time, upgrade id
        source.readBytes( byteBuffer );
        byteBuffer.flip();
        // read order matters - see Server.writeStoreId() for version 2.1.4
        long creationTime = byteBuffer.getLong();
        long randomId = byteBuffer.getLong();
        long storeVersion = byteBuffer.getLong();
        long upgradeTime = byteBuffer.getLong();
        long upgradeId = byteBuffer.getLong();
        return new StoreId( creationTime, randomId, storeVersion, upgradeTime, upgradeId );
    }
}
