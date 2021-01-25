/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.storeid;

import io.netty.handler.codec.DecoderException;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.SafeChannelMarshal;
import org.neo4j.storageengine.api.StoreId;

public final class StoreIdMarshal extends SafeChannelMarshal<StoreId>
{
    public static final StoreIdMarshal INSTANCE = new StoreIdMarshal();

    private StoreIdMarshal() {}

    @Override
    public void marshal( StoreId storeId, WritableChannel channel ) throws IOException
    {
        if ( storeId == null )
        {
            channel.put( (byte) 0 );
            return;
        }

        channel.put( (byte) 1 );
        channel.putLong( storeId.getCreationTime() );
        channel.putLong( storeId.getRandomId() );
        channel.putLong( storeId.getStoreVersion() );
        channel.putLong( storeId.getUpgradeTime() );
        channel.putLong( storeId.getUpgradeTxId() );
    }

    @Override
    protected StoreId unmarshal0( ReadableChannel channel ) throws IOException
    {
        byte exists = channel.get();
        if ( exists == 0 )
        {
            return null;
        }
        else if ( exists != 1 )
        {
            throw new DecoderException( "Unexpected value: " + exists );
        }

        long creationTime = channel.getLong();
        long randomId = channel.getLong();
        long storeVersion = channel.getLong();
        long upgradeTime = channel.getLong();
        long upgradeId = channel.getLong();
        return new StoreId( creationTime, randomId, storeVersion, upgradeTime, upgradeId );
    }
}
