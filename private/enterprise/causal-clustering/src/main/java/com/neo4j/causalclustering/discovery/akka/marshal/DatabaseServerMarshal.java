/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseServer;

import java.io.IOException;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

public class DatabaseServerMarshal extends SafeChannelMarshal<DatabaseServer>
{
    public static final DatabaseServerMarshal INSTANCE = new DatabaseServerMarshal();

    private DatabaseServerMarshal()
    {
    }

    @Override
    protected DatabaseServer unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        var serverId = ServerId.Marshal.INSTANCE.unmarshal( channel );
        return new DatabaseServer( databaseId, serverId );
    }

    @Override
    public void marshal( DatabaseServer databaseServer, WritableChannel channel ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( databaseServer.databaseId(), channel );
        ServerId.Marshal.INSTANCE.marshal( databaseServer.serverId(), channel );
    }
}
