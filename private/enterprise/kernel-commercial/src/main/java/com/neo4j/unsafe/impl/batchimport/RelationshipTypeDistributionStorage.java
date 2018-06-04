/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.unsafe.impl.batchimport.DataStatistics;
import org.neo4j.unsafe.impl.batchimport.DataStatistics.RelationshipTypeCount;

import static org.neo4j.io.fs.OpenMode.READ;
import static org.neo4j.io.fs.OpenMode.READ_WRITE;

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
        try ( FlushableChannel channel = new PhysicalFlushableChannel( fs.open( file, READ_WRITE ) ) )
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
        try ( ReadableClosableChannel channel = new ReadAheadChannel<>( fs.open( file, READ ) ) )
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
