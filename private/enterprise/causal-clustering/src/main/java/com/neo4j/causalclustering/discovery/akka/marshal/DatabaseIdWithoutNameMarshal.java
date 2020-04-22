/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.UUIDMarshal;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;

public class DatabaseIdWithoutNameMarshal extends SafeChannelMarshal<DatabaseId>
{
    public static final DatabaseIdWithoutNameMarshal INSTANCE = new DatabaseIdWithoutNameMarshal();

    @Override
    protected DatabaseId unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        return DatabaseIdFactory.from( UUIDMarshal.INSTANCE.unmarshal( channel ) );
    }

    @Override
    public void marshal( DatabaseId databaseIdRaw, WritableChannel channel ) throws IOException
    {
        UUIDMarshal.INSTANCE.marshal( databaseIdRaw.uuid(), channel );
    }
}
