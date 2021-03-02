/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.Address;
import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

/**
 * One might expect Akka to be able to serialize without Java serialization, but no.
 * This is not to be confused with {@link akka.remote.UniqueAddress} which does have an Akka serializer.
 */
public final class UniqueAddressMarshal extends SafeChannelMarshal<UniqueAddress>
{
    public static UniqueAddressMarshal INSTANCE = new UniqueAddressMarshal();

    private UniqueAddressMarshal()
    {
    }

    @Override
    protected UniqueAddress unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        String protocol = StringMarshal.unmarshal( channel );
        String system = StringMarshal.unmarshal( channel );
        String host = StringMarshal.unmarshal( channel );
        int port = channel.getInt();
        long uid = channel.getLong();

        Address address;
        if ( host != null ) // If host is defined then port must be too
        {
            address = new Address( protocol, system, host, port );
        }
        else
        {
            address = new Address( protocol, system );
        }
        return new UniqueAddress( address, uid );
    }

    @Override
    public void marshal( UniqueAddress uniqueAddress, WritableChannel channel ) throws IOException
    {
        Address address = uniqueAddress.address();

        StringMarshal.marshal( channel, address.protocol() );
        StringMarshal.marshal( channel, address.system() );
        if ( address.host().isDefined() )
        {
            StringMarshal.marshal( channel, address.host().get() );
        }
        else
        {
            StringMarshal.marshal( channel, null );
        }
        if ( address.port().isDefined() )
        {
            channel.putInt( (Integer) address.port().get() );
        }
        else
        {
            channel.putInt( -1 );
        }
        channel.putLong( uniqueAddress.longUid() );
    }
}
