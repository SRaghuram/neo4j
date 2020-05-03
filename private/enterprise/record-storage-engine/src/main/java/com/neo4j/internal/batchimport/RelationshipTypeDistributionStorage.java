/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import java.io.File;
import java.io.IOException;

import org.neo4j.internal.batchimport.DataStatistics;
import org.neo4j.internal.batchimport.DataStatistics.RelationshipTypeCount;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.memory.MemoryTracker;

class RelationshipTypeDistributionStorage
{
    private final FileSystemAbstraction fs;
    private final File file;
    private final MemoryTracker memoryTracker;

    RelationshipTypeDistributionStorage( FileSystemAbstraction fs, File file, MemoryTracker memoryTracker )
    {
        this.fs = fs;
        this.file = file;
        this.memoryTracker = memoryTracker;
    }

    void store( DataStatistics distribution ) throws IOException
    {
        // This could have been done using a writer and writing human readable text.
        // Perhaps simpler code, but this format is safe against any type of weird characters that the type may contain
        try ( FlushableChannel channel = new PhysicalFlushableChannel( fs.write( file ), memoryTracker ) )
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
        try ( NativeScopedBuffer bufferScope = new NativeScopedBuffer( ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE, memoryTracker );
              ReadableChannel channel = new ReadAheadChannel<>( fs.read( file ), bufferScope.getBuffer() ) )
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
