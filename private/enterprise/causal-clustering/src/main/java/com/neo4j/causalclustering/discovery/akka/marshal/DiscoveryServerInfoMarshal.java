/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.configuration.ServerGroupName;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;
import org.neo4j.kernel.database.DatabaseId;

abstract class DiscoveryServerInfoMarshal<T extends DiscoveryServerInfo> extends SafeChannelMarshal<T>
{
    static Set<ServerGroupName> unmarshalGroups( ReadableChannel channel ) throws IOException
    {
        var size = channel.getInt();
        var groups = new String[size];
        for ( int i = 0; i < size; i++ )
        {
            groups[i] = StringMarshal.unmarshal( channel );
        }
        return ServerGroupName.setOf( groups );
    }

    static void marshalGroups( DiscoveryServerInfo info, WritableChannel channel ) throws IOException
    {
        var groups = info.groups();
        channel.putInt( groups.size() );
        for ( var group : groups )
        {
            StringMarshal.marshal( channel, group.getRaw() );
        }
    }

    static Set<DatabaseId> unmarshalDatabaseIds( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var size = channel.getInt();
        var databaseIds = new HashSet<DatabaseId>( size );
        for ( int i = 0; i < size; i++ )
        {
            databaseIds.add( DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel ) );
        }
        return databaseIds;
    }

    static void marshalDatabaseIds( DiscoveryServerInfo info, WritableChannel channel ) throws IOException
    {
        var databaseIds = info.startedDatabaseIds();
        channel.putInt( databaseIds.size() );
        for ( var databaseId : databaseIds )
        {
            DatabaseIdWithoutNameMarshal.INSTANCE.marshal( databaseId, channel );
        }
    }
}
