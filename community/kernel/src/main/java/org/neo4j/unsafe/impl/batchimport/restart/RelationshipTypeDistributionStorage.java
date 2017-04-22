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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.unsafe.impl.batchimport.DataStatistics;
import org.neo4j.unsafe.impl.batchimport.DataStatistics.RelationshipTypeCount;

class RelationshipTypeDistributionStorage
{
    private final FileSystemAbstraction fs;
    private final File file;

    RelationshipTypeDistributionStorage( FileSystemAbstraction fs, File file )
    {
        this.fs = fs;
        this.file = file;
    }

    void store( DataStatistics distribution ) throws IOException
    {
        // This could have been done using a writer and writing human readable text.
        // Perhaps simpler code, but this format is safe against any type of weird characters that the type may contain
        try ( FlushableChannel channel = new PhysicalFlushableChannel( fs.open( file, "rw" ) ) )
        {
            channel.putLong( distribution.getNodeCount() );
            channel.putLong( distribution.getPropertyCount() );
            channel.putInt( distribution.getNumberOfRelationshipTypes() );
            for ( RelationshipTypeCount entry : distribution )
            {
                channel.putInt( entry.getTypeId() );
                channel.putLong( entry.getCount() );
            }
        }
    }

    DataStatistics load() throws IOException
    {
        try ( ReadableClosableChannel channel = new ReadAheadChannel<>( fs.open( file, "r" ) ) )
        {
            long nodeCount = channel.getLong();
            long propertyCount = channel.getLong();
            RelationshipTypeCount[] entries = new RelationshipTypeCount[channel.getInt()];
            for ( int i = 0; i < entries.length; i++ )
            {
                int typeId = channel.getInt();
                long count = channel.getLong();
                entries[i] = new RelationshipTypeCount( typeId, count );
            }
            return new DataStatistics( nodeCount, propertyCount, entries );
        }
    }

    void remove()
    {
        fs.deleteFile( file );
    }
}
