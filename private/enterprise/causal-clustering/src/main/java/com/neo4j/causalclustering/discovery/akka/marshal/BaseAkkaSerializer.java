/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.remote.MessageSerializer;
import akka.serialization.JSerializer;
import com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService;
import com.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;

/**
 * Include in {@link TypesafeConfigService} to register with Akka
 */
public abstract class BaseAkkaSerializer<T> extends JSerializer
{
    // Serializer identifiers. Must be unique, not change across versions, be above 40.
    static final int LEADER_INFO = 1000;
    static final int RAFT_ID = 1001;
    static final int UNIQUE_ADDRESS = 1002;
    static final int CORE_SERVER_INFO_FOR_MEMBER_ID = 1003;
    static final int READ_REPLICA_INFO_FOR_MEMBER_ID = 1004;
    static final int MEMBER_ID = 1005;
    static final int READ_REPLICA_INFO = 1006;
    static final int CORE_TOPOLOGY = 1007;
    static final int READ_REPLICA_REMOVAL = 1008;
    static final int READ_REPLICA_TOPOLOGY = 1009;
    static final int DB_LEADER_INFO = 1010;
    static final int REPLICATED_LEADER_INFO = 1011;
    static final int DATABASE_ID = 1012;
    static final int REPLICATED_DATABASE_STATE = 1013;
    static final int DATABASE_TO_MEMBER = 1014;
    static final int DATABASE_STATE = 1015;

    private final ChannelMarshal<T> marshal;
    private final int id;
    private final int sizeHint;

    protected BaseAkkaSerializer( ChannelMarshal<T> marshal, int id, int sizeHint )
    {
        this.marshal = marshal;
        this.id = id;
        this.sizeHint = sizeHint;
    }

    @Override
    public byte[] toBinary( Object o )
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( sizeHint );
        OutputStreamWritableChannel channel = new OutputStreamWritableChannel( outputStream );

        try
        {
            marshal.marshal( (T) o, channel );
        }
        catch ( IOException e )
        {
            throw new MessageSerializer.SerializationException( "Failed to serialize", e );
        }

        return outputStream.toByteArray();
    }

    @Override
    public Object fromBinaryJava( byte[] bytes, Class<?> manifest )
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream( bytes );
        InputStreamReadableChannel channel = new InputStreamReadableChannel( inputStream );

        try
        {
            return marshal.unmarshal( channel );
        }
        catch ( IOException | EndOfStreamException e )
        {
            throw new MessageSerializer.SerializationException( "Failed to deserialize", e );
        }
    }

    int sizeHint()
    {
        return sizeHint;
    }

    @Override
    public int identifier()
    {
        return id;
    }

    @Override
    public boolean includeManifest()
    {
        return false;
    }
}
