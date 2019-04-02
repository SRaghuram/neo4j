/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.io.fs.ReadableClosableChannel;

import static java.nio.file.StandardOpenOption.READ;
import static java.util.Set.of;

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
        try ( FlushableChannel channel = new PhysicalFlushableChannel( fs.create( file ) ) )
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
        try ( ReadableClosableChannel channel = new ReadAheadChannel<>( fs.open( file, of( READ ) ) ) )
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
