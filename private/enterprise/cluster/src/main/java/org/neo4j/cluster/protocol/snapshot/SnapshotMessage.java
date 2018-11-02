/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.snapshot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;

/**
 * TODO
 */
public enum SnapshotMessage
        implements MessageType
{
    join, leave,
    setSnapshotProvider,
    refreshSnapshot,
    sendSnapshot, snapshot;

    // TODO This needs to be replaced with something that can handle bigger snapshots
    public static class SnapshotState
            implements Serializable
    {
        private static final long serialVersionUID = 1518479578399690929L;

        private long lastDeliveredInstanceId = -1;
        transient SnapshotProvider provider;

        private transient ObjectOutputStreamFactory objectOutputStreamFactory;

        transient byte[] buf;

        public SnapshotState( long lastDeliveredInstanceId, SnapshotProvider provider,
                ObjectOutputStreamFactory objectOutputStreamFactory )
        {
            this.lastDeliveredInstanceId = lastDeliveredInstanceId;
            this.provider = provider;

            if ( objectOutputStreamFactory == null )
            {
                throw new RuntimeException( "objectOutputStreamFactory was null" );
            }

            this.objectOutputStreamFactory = objectOutputStreamFactory;
        }

        public long getLastDeliveredInstanceId()
        {
            return lastDeliveredInstanceId;
        }

        public void setState( SnapshotProvider provider, ObjectInputStreamFactory objectInputStreamFactory )
        {
            ByteArrayInputStream bin = new ByteArrayInputStream( buf );

            try ( ObjectInputStream oin = objectInputStreamFactory.create( bin ) )
            {
                provider.setState( oin );
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
            }
        }

        private void writeObject( java.io.ObjectOutputStream out )
                throws IOException
        {
            out.defaultWriteObject();
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            ObjectOutputStream oout = objectOutputStreamFactory.create( bout );
            provider.getState( oout );
            oout.close();
            byte[] buf = bout.toByteArray();
            out.writeInt( buf.length );
            out.write( buf );
        }

        private void readObject( java.io.ObjectInputStream in )
                throws IOException, ClassNotFoundException
        {
            in.defaultReadObject();
            buf = new byte[in.readInt()];
            try
            {
                in.readFully( buf );
            }
            catch ( EOFException endOfFile )
            {
                // do nothing - the stream's ended but the message content got through ok.
            }
        }
    }
}
