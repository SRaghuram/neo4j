/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Serializes and deserializes value to/from Payloads.
 */
public class AtomicBroadcastSerializer
{
    private ObjectInputStreamFactory objectInputStreamFactory;
    private ObjectOutputStreamFactory objectOutputStreamFactory;

    public AtomicBroadcastSerializer()
    {
        this( new ObjectStreamFactory(), new ObjectStreamFactory() );
    }

    public AtomicBroadcastSerializer( ObjectInputStreamFactory objectInputStreamFactory,
            ObjectOutputStreamFactory objectOutputStreamFactory )
    {
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
    }

    public Payload broadcast( Object value )
            throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        byte[] bytes;
        try ( ObjectOutputStream oout = objectOutputStreamFactory.create( bout ) )
        {
            oout.writeObject( value );
        }
        bytes = bout.toByteArray();
        return new Payload( bytes, bytes.length );
    }

    public Object receive( Payload payload )
            throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream in = new ByteArrayInputStream( payload.getBuf(), 0, payload.getLen() );
        try ( ObjectInputStream oin = objectInputStreamFactory.create( in ) )
        {
            return oin.readObject();
        }
    }
}
