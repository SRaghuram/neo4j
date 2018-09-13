/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v1.storecopy;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.string.UTF8;

public class GetStoreFileRequestMarshalV1 extends SafeChannelMarshal<GetStoreFileRequest>
{
    private String databaseName;

    GetStoreFileRequestMarshalV1( String databaseName )
    {
        this.databaseName = databaseName;
    }

    @Override
    protected GetStoreFileRequest unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( channel );
        long requiredTransactionId = channel.getLong();
        int fileNameLength = channel.getInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        channel.get( fileNameBytes, fileNameLength );
        return new GetStoreFileRequest( storeId, new File( UTF8.decode( fileNameBytes ) ), requiredTransactionId, databaseName );
    }

    @Override
    public void marshal( GetStoreFileRequest getStoreFileRequest, WritableChannel channel ) throws IOException
    {
        StoreIdMarshal.INSTANCE.marshal( getStoreFileRequest.expectedStoreId(), channel );
        channel.putLong( getStoreFileRequest.requiredTransactionId() );
        String name = getStoreFileRequest.file().getName();
        channel.putInt( name.length() );
        channel.put( UTF8.encode( name ), name.length() );
    }
}
