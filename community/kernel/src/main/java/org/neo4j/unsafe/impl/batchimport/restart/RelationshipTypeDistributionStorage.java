/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.restart;

import org.apache.commons.codec.Charsets;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.unsafe.impl.batchimport.RelationshipTypeDistribution;

import static org.neo4j.io.ByteUnit.kibiBytes;

class RelationshipTypeDistributionStorage
{
    private final FileSystemAbstraction fs;
    private final File file;
    private final ByteBuffer buffer = ByteBuffer.allocate( (int) kibiBytes( 1 ) ); // that size should be good for everything, right?

    RelationshipTypeDistributionStorage( FileSystemAbstraction fs, File file )
    {
        this.fs = fs;
        this.file = file;
    }

    void store( RelationshipTypeDistribution distribution ) throws IOException
    {
        // This could have been done using a writer and writing human readable text.
        // Perhaps simpler code, but this format is safe against any type of weird characters that the type may contain
        try ( StoreChannel channel = fs.open( file, "rw" ) )
        {
            flush( buffer.putInt( distribution.getNumberOfRelationshipTypes() ), channel );
            distribution.forEach( entry ->
            {
                // write key
                Object key = entry.first();
                if ( key instanceof String )
                {
                    buffer.put( (byte) 0 );
                    writeString( buffer, (String) key );
                }
                else
                {
                    buffer.put( (byte) 1 );
                    buffer.putInt( (Integer) key );
                }

                // write value
                buffer.putLong( entry.other() );

                // flush to channel
                try
                {
                    flush( buffer, channel );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            } );
        }
    }

    RelationshipTypeDistribution load() throws IOException
    {
        try ( StoreChannel channel = fs.open( file, "r" ) )
        {
            @SuppressWarnings( "unchecked" )
            Pair<Object,Long>[] entries = new Pair[read( channel, Integer.BYTES ).getInt()];
            for ( int i = 0; i < entries.length; i++ )
            {
                Object key = read( channel, Byte.BYTES ).get() == 0 ? readString( channel ) : read( channel, Integer.BYTES ).getInt();
                Long value = read( channel, Long.BYTES ).getLong();
                entries[i] = Pair.of( key, value );
            }
            return new RelationshipTypeDistribution( entries );
        }
    }

    private String readString( StoreChannel channel ) throws IOException
    {
        byte[] bytes = new byte[read( channel, Integer.BYTES ).getInt()];
        read( channel, bytes.length ).get( bytes );
        return new String( bytes, Charsets.UTF_8 );
    }

    private ByteBuffer read( StoreChannel channel, int bytes ) throws IOException
    {
        buffer.clear();
        buffer.limit( bytes );
        int read = 0;
        do
        {
            int readChunk = channel.read( buffer );
            if ( readChunk == -1 )
            {
                throw new IOException( "Unexpected end of file" );
            }
            read += readChunk;
        }
        while ( read < bytes );
        buffer.flip();
        return buffer;
    }

    private static void writeString( ByteBuffer buffer, String string )
    {
        byte[] bytes = string.getBytes( Charsets.UTF_8 );
        buffer.putInt( bytes.length );
        buffer.put( bytes );
    }

    private static void flush( ByteBuffer buffer, StoreChannel channel ) throws IOException
    {
        buffer.flip();
        channel.writeAll( buffer );
        buffer.clear();
    }
}
