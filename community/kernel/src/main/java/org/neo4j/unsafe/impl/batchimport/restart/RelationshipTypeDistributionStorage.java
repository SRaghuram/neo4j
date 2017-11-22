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

import java.io.File;
import java.io.IOException;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.unsafe.impl.batchimport.RelationshipTypeDistribution;

import static org.neo4j.unsafe.impl.batchimport.restart.ChannelUtils.readString;
import static org.neo4j.unsafe.impl.batchimport.restart.ChannelUtils.writeString;

class RelationshipTypeDistributionStorage
{
    private final FileSystemAbstraction fs;
    private final File file;

    RelationshipTypeDistributionStorage( FileSystemAbstraction fs, File file )
    {
        this.fs = fs;
        this.file = file;
    }

    void store( RelationshipTypeDistribution distribution ) throws IOException
    {
        // This could have been done using a writer and writing human readable text.
        // Perhaps simpler code, but this format is safe against any type of weird characters that the type may contain
        try ( FlushableChannel channel = new PhysicalFlushableChannel( fs.open( file, "rw" ) ) )
        {
            channel.putInt( distribution.getNumberOfRelationshipTypes() );
            for ( Pair<Object,Long> entry : distribution )
            {
                // write key
                Object key = entry.first();
                if ( key instanceof String )
                {
                    channel.put( (byte) 0 );
                    writeString( (String) key, channel );
                }
                else
                {
                    channel.put( (byte) 1 );
                    channel.putInt( (Integer) key );
                }

                // write value
                channel.putLong( entry.other() );
            }
        }
    }

    RelationshipTypeDistribution load() throws IOException
    {
        try ( ReadableClosableChannel channel = new ReadAheadChannel<>( fs.open( file, "r" ) ) )
        {
            @SuppressWarnings( "unchecked" )
            Pair<Object,Long>[] entries = new Pair[channel.getInt()];
            for ( int i = 0; i < entries.length; i++ )
            {
                Object key = channel.get() == 0 ? readString( channel ) : channel.getInt();
                Long value = channel.getLong();
                entries[i] = Pair.of( key, value );
            }
            return new RelationshipTypeDistribution( entries );
        }
    }

    void remove()
    {
        fs.deleteFile( file );
    }
}
