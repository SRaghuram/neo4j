/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.messaging.EndOfStreamException;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.database.DatabaseId;

public class DatabaseIdMarshal extends SafeChannelMarshal<DatabaseId>
{
    public static final DatabaseIdMarshal INSTANCE = new DatabaseIdMarshal();

    @Override
    protected DatabaseId unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        return new DatabaseId( StringMarshal.unmarshal( channel ) );
    }

    @Override
    public void marshal( DatabaseId databaseId, WritableChannel channel ) throws IOException
    {
        StringMarshal.marshal( channel, databaseId.name() );
    }
}
