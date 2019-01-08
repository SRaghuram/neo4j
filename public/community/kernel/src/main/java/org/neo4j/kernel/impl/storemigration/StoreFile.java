/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Predicate;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.string.UTF8;

import static org.neo4j.helpers.collection.Iterables.iterable;

public enum StoreFile
{
    // all store files in Neo4j
    NODE_STORE(
            NodeStore.TYPE_DESCRIPTOR,
            StoreFactory.NODE_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    NODE_LABEL_STORE(
            DynamicArrayStore.TYPE_DESCRIPTOR,
            StoreFactory.NODE_LABELS_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    PROPERTY_STORE(
            PropertyStore.TYPE_DESCRIPTOR,
            StoreFactory.PROPERTY_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    PROPERTY_ARRAY_STORE(
            DynamicArrayStore.TYPE_DESCRIPTOR,
            StoreFactory.PROPERTY_ARRAYS_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    PROPERTY_STRING_STORE(
            DynamicStringStore.TYPE_DESCRIPTOR,
            StoreFactory.PROPERTY_STRINGS_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    PROPERTY_KEY_TOKEN_STORE(
            PropertyKeyTokenStore.TYPE_DESCRIPTOR,
            StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    PROPERTY_KEY_TOKEN_NAMES_STORE(
            DynamicStringStore.TYPE_DESCRIPTOR,
            StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    RELATIONSHIP_STORE(
            RelationshipStore.TYPE_DESCRIPTOR,
            StoreFactory.RELATIONSHIP_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    RELATIONSHIP_GROUP_STORE(
            RelationshipGroupStore.TYPE_DESCRIPTOR,
            StoreFactory.RELATIONSHIP_GROUP_STORE_NAME,
            StandardV2_1.STORE_VERSION
    ),

    RELATIONSHIP_TYPE_TOKEN_STORE(
            RelationshipTypeTokenStore.TYPE_DESCRIPTOR,
            StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    RELATIONSHIP_TYPE_TOKEN_NAMES_STORE(
            DynamicStringStore.TYPE_DESCRIPTOR,
            StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    LABEL_TOKEN_STORE(
            LabelTokenStore.TYPE_DESCRIPTOR,
            StoreFactory.LABEL_TOKEN_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    LABEL_TOKEN_NAMES_STORE(
            DynamicStringStore.TYPE_DESCRIPTOR,
            StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    SCHEMA_STORE(
            SchemaStore.TYPE_DESCRIPTOR,
            StoreFactory.SCHEMA_STORE_NAME,
            StandardV2_0.STORE_VERSION
    ),

    COUNTS_STORE_LEFT(
            CountsTracker.TYPE_DESCRIPTOR,
            StoreFactory.COUNTS_STORE + CountsTracker.LEFT,
            StandardV2_2.STORE_VERSION,
            false
    )
            {
                @Override
                boolean isOptional()
                {
                    return true;
                }
            },
    COUNTS_STORE_RIGHT(
            CountsTracker.TYPE_DESCRIPTOR,
            StoreFactory.COUNTS_STORE + CountsTracker.RIGHT,
            StandardV2_2.STORE_VERSION,
            false
    )
            {
                @Override
                boolean isOptional()
                {
                    return true;
                }
            },

    NEO_STORE(
            MetaDataStore.TYPE_DESCRIPTOR,
            "",
            StandardV2_0.STORE_VERSION
    );

    private final String typeDescriptor;
    private final String storeFileNamePart;
    private final String sinceVersion;
    private final boolean recordStore;

    StoreFile( String typeDescriptor, String storeFileNamePart, String sinceVersion )
    {
        this( typeDescriptor, storeFileNamePart, sinceVersion, true );
    }

    StoreFile( String typeDescriptor, String storeFileNamePart, String sinceVersion, boolean recordStore )
    {
        this.typeDescriptor = typeDescriptor;
        this.storeFileNamePart = storeFileNamePart;
        this.sinceVersion = sinceVersion;
        this.recordStore = recordStore;
    }

    public String forVersion( String version )
    {
        return typeDescriptor + " " + version;
    }

    public String fileName( StoreFileType type )
    {
        return type.augment( MetaDataStore.DEFAULT_NAME + storeFileNamePart );
    }

    public String storeFileName()
    {
        return fileName( StoreFileType.STORE );
    }

    public String fileNamePart()
    {
        return storeFileNamePart;
    }

    public boolean isRecordStore()
    {
        return recordStore;
    }

    public static Iterable<StoreFile> legacyStoreFilesForVersion( final String version )
    {
        Predicate<StoreFile> predicate = item -> version.compareTo( item.sinceVersion ) >= 0;
        Iterable<StoreFile> storeFiles = currentStoreFiles();
        Iterable<StoreFile> filter = Iterables.filter( predicate, storeFiles );
        return filter;
    }

    public static Iterable<StoreFile> currentStoreFiles()
    {
        return Iterables.iterable( values() );
    }

    public static void fileOperation( FileOperation operation, FileSystemAbstraction fs, File fromDirectory,
            File toDirectory, StoreFile... files ) throws IOException
    {
        fileOperation( operation, fs, fromDirectory, toDirectory, storeFiles( files ), false,
                ExistingTargetStrategy.FAIL );
    }

    public static void fileOperation( FileOperation operation, FileSystemAbstraction fs, File fromDirectory,
            File toDirectory, Iterable<StoreFile> files,
            boolean allowSkipNonExistentFiles, ExistingTargetStrategy existingTargetStrategy ) throws IOException
    {
        fileOperation( operation, fs, fromDirectory, toDirectory, files, allowSkipNonExistentFiles,
                existingTargetStrategy, StoreFileType.values() );
    }

    /**
     * Performs a file operation on a database's store files from one directory
     * to another. Remember that in the case of {@link FileOperation#MOVE moving files}, the way that's done is to
     * just rename files (the standard way of moving with JDK6) from and to must be on the same disk partition.
     *
     * @param fromDirectory directory that hosts the database files.
     * @param toDirectory directory to receive the database files.
     * @throws IOException if any of the operations fail for any reason.
     */
    public static void fileOperation( FileOperation operation, FileSystemAbstraction fs, File fromDirectory,
            File toDirectory, Iterable<StoreFile> files,
            boolean allowSkipNonExistentFiles, ExistingTargetStrategy existingTargetStrategy,
            StoreFileType... types ) throws IOException
    {
        // TODO: change the order of files to handle failure conditions properly
        for ( StoreFile storeFile : files )
        {
            for ( StoreFileType type : types )
            {
                String fileName = storeFile.fileName( type );
                operation.perform( fs, fileName,
                        fromDirectory, allowSkipNonExistentFiles, toDirectory, existingTargetStrategy );
            }
        }
    }

    public static void removeTrailers( String version, FileSystemAbstraction fs, File storeDir, int pageSize )
            throws IOException
    {
        for ( StoreFile storeFile : legacyStoreFilesForVersion( StandardV3_0.STORE_VERSION ) )
        {
            String trailer = storeFile.forVersion( version );
            byte[] encodedTrailer = UTF8.encode( trailer );
            File file = new File( storeDir, storeFile.storeFileName() );
            long fileSize = fs.getFileSize( file );
            long truncationPosition = containsTrailer( fs, file, fileSize, pageSize, encodedTrailer );
            if ( truncationPosition != -1 )
            {
                fs.truncate( file, truncationPosition );
            }
        }
    }

    private static long containsTrailer( FileSystemAbstraction fs, File file, long fileSize, int pageSize,
            byte[] encodedTrailer ) throws IOException
    {
        if ( !fs.fileExists( file ) )
        {
            return -1L;
        }

        try ( StoreChannel channel = fs.open( file, "rw" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( encodedTrailer.length );
            long newPosition = Math.max( 0, fileSize - encodedTrailer.length );
            long stopPosition = Math.max( 0, fileSize - encodedTrailer.length - pageSize );
            while ( newPosition >= stopPosition )
            {
                channel.position( newPosition );
                int totalRead = 0;
                do
                {
                    int read = channel.read( buffer );
                    if ( read == -1 )
                    {
                        return -1L;
                    }
                    totalRead += read;
                }
                while ( totalRead < encodedTrailer.length );

                if ( Arrays.equals( buffer.array(), encodedTrailer ) )
                {
                    return newPosition;
                }
                else
                {
                    newPosition -= 1;
                    buffer.clear();
                }
            }

            return -1;
        }
    }

    boolean isOptional()
    {
        return false;
    }

    /**
     * Merely here as convenience since java generics is acting up in many cases, so this is nicer for
     * inlining such a call into {@link #fileOperation(FileOperation, FileSystemAbstraction, File, File, Iterable, boolean, boolean, StoreFileType...)}
     */
    public static Iterable<StoreFile> storeFiles( StoreFile... files )
    {
        return iterable( files );
    }
}
