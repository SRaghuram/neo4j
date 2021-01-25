/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.causalclustering.messaging.marshalling.UUIDMarshal;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Consider using {@link DatabaseIdWithoutNameMarshal} instead. This is only intended to be used when the name *has* to be marshalled as well.
 */
public class InefficientNamedDatabaseIdMarshal extends SafeChannelMarshal<NamedDatabaseId>
{
    public static final InefficientNamedDatabaseIdMarshal INSTANCE = new InefficientNamedDatabaseIdMarshal();

    @Override
    protected NamedDatabaseId unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        return DatabaseIdFactory.from( StringMarshal.unmarshal( channel ), UUIDMarshal.INSTANCE.unmarshal( channel ) );
    }

    @Override
    public void marshal( NamedDatabaseId namedDatabaseId, WritableChannel channel ) throws IOException
    {
        StringMarshal.marshal( channel, namedDatabaseId.name() );
        UUIDMarshal.INSTANCE.marshal( namedDatabaseId.databaseId().uuid(), channel );
    }
}
