/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor;

public class PhysicalLogVersionedStoreChannel extends DelegatingStoreChannel implements LogVersionedStoreChannel
{
    private final long version;
    private final byte formatVersion;
    private long position;
    private final File file;
    private final ChannelNativeAccessor nativeChannelAccessor;

    public PhysicalLogVersionedStoreChannel( StoreChannel delegateChannel, long version, byte formatVersion, File file,
            ChannelNativeAccessor nativeChannelAccessor ) throws IOException
    {
        super( delegateChannel );
        this.version = version;
        this.formatVersion = formatVersion;
        this.position = delegateChannel.position();
        this.file = file;
        this.nativeChannelAccessor = nativeChannelAccessor;
    }

    public File getFile()
    {
        return file;
    }

    @Override
    public void writeAll( ByteBuffer src, long position )
    {
        throw new UnsupportedOperationException( "Not needed" );
    }

    @Override
    public void writeAll( ByteBuffer src ) throws IOException
    {
        advance( src.remaining() );
        super.writeAll( src );
    }

    @Override
    public int read( ByteBuffer dst, long position )
    {
        throw new UnsupportedOperationException( "Not needed" );
    }

    @Override
    public StoreChannel position( long newPosition ) throws IOException
    {
        this.position = newPosition;
        return super.position( newPosition );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        return (int) advance( super.read( dst ) );
    }

    private long advance( long bytes )
    {
        if ( bytes != -1 )
        {
            position += bytes;
        }
        return bytes;
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        return (int) advance( super.write( src ) );
    }

    @Override
    public long position()
    {
        return position;
    }

    @Override
    public void close() throws IOException
    {
        nativeChannelAccessor.evictFromSystemCache( this, version );
        super.close();
    }

    @Override
    public long write( ByteBuffer[] sources, int offset, int length ) throws IOException
    {
        return advance( super.write( sources, offset, length ) );
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        return advance( super.write( srcs ) );
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        return advance( super.read( dsts, offset, length ) );
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        return advance( super.read( dsts ) );
    }

    @Override
    public long getVersion()
    {
        return version;
    }

    @Override
    public byte getLogFormatVersion()
    {
        return formatVersion;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        PhysicalLogVersionedStoreChannel that = (PhysicalLogVersionedStoreChannel) o;

        return version == that.version && delegate.equals( that.delegate );
    }

    @Override
    public int hashCode()
    {
        int result = delegate.hashCode();
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }
}
