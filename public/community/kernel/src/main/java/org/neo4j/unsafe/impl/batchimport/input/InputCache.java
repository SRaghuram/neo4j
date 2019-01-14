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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;

import static org.neo4j.io.fs.OpenMode.READ;
import static org.neo4j.io.fs.OpenMode.READ_WRITE;

/**
 * Cache of streams of nodes and relationships from an {@link Input} instance.
 * Useful since {@link ParallelBatchImporter} may require multiple passes over the input data and so
 * consecutive passes will be served by this cache instead, for {@link InputIterable} that does not
 * {@link InputIterable#supportsMultiplePasses() support multiple passes}.
 *
 * <pre>
 * Properties format:
 * - 2B property count, or {@link #HAS_FIRST_PROPERTY_ID} or {@link #END_OF_ENTITIES}
 * - property...:
 *   - 4B token id (token string->id mapping is in header file)
 *   - ?B value, see {@link ValueType}
 * </pre>
 *
 * <pre>
 * Group format:
 * - 1B, {@link #SAME_GROUP} or {@link #NEW_GROUP}
 *   IF {@link #NEW_GROUP}
 *   - 4B group if (if {@link #NEW_GROUP}
 *   - 4B token id
 * </pre>
 *
 * <pre>
 * Node format:
 * - properties (see "Properties format")
 * - group (see "Group format")
 * - id
 *   - ?B id value, see {@link ValueType}
 * - labels
 *   - 1B label mode, {@link #HAS_LABEL_FIELD} or {@link #LABEL_ADDITION} or {@link #LABEL_REMOVAL} or
 *     {@link #END_OF_LABEL_CHANGES}
 *     WHILE NOT {@link #END_OF_LABEL_CHANGES}
 *     - 4B token id, to add or remove
 *     - 1B label mode, next mode
 * </pre>
 *
 * <pre>
 * Relationship format:
 * - properties (see "Properties format")
 * - start node group (see "Group format")
 * - end node group (see "Group format")
 * - start node id
 *   - ?B id value, see {@link ValueType}
 * - end node id
 *   - ?B id value, see {@link ValueType}
 * - type
 *   - 1B mode, {@link #SAME_TYPE} or {@link #NEW_TYPE} or {@link #HAS_TYPE_ID}
 *     IF {@link #HAS_TYPE_ID}
 *     4B type id
 *     ELSE IF {@link #NEW_TYPE}
 *     4B token id
 * </pre>
 *
 * The format stores entities in batches, each batch having a small header containing number of bytes
 * and number of entities.
 */
public class InputCache implements Closeable
{
    private static final String HEADER = "-header";
    private static final String NODES = "nodes";
    private static final String RELATIONSHIPS = "relationships";
    private static final String NODES_HEADER = NODES + HEADER;
    private static final String RELATIONSHIPS_HEADER = RELATIONSHIPS + HEADER;

    static final byte SAME_GROUP = 0;
    static final byte NEW_GROUP = 1;
    static final byte PROPERTY_KEY_TOKEN = 0;
    static final byte LABEL_TOKEN = 1;
    static final byte RELATIONSHIP_TYPE_TOKEN = 2;
    static final byte GROUP_TOKEN = 3;
    static final byte HIGH_TOKEN_TYPE = 4;
    static final short HAS_FIRST_PROPERTY_ID = -1;
    static final byte HAS_LABEL_FIELD = 3;
    static final byte LABEL_REMOVAL = 1;
    static final byte LABEL_ADDITION = 2;
    static final byte END_OF_LABEL_CHANGES = 0;
    static final byte HAS_TYPE_ID = 2;
    static final byte SAME_TYPE = 0;
    static final byte NEW_TYPE = 1;
    static final byte END_OF_HEADER = -2;
    static final short END_OF_ENTITIES = -3;

    private final FileSystemAbstraction fs;
    private final File cacheDirectory;
    private final RecordFormats recordFormats;
    private final int chunkSize;

    /**
     * @param fs {@link FileSystemAbstraction} to use
     * @param cacheDirectory directory for placing the cached files
     * @param recordFormats which {@link RecordFormats format} records are in
     * @param chunkSize rough size of chunks written to the cache
     */
    public InputCache( FileSystemAbstraction fs, File cacheDirectory, RecordFormats recordFormats, int chunkSize )
    {
        this.fs = fs;
        this.cacheDirectory = cacheDirectory;
        this.recordFormats = recordFormats;
        this.chunkSize = chunkSize;
    }

    public InputCacher cacheNodes() throws IOException
    {
        return new InputNodeCacheWriter( channel( NODES, READ_WRITE ), channel( NODES_HEADER, READ_WRITE ),
                recordFormats, chunkSize );
    }

    public InputCacher cacheRelationships() throws IOException
    {
        return new InputRelationshipCacheWriter( channel( RELATIONSHIPS, READ_WRITE ),
                channel( RELATIONSHIPS_HEADER, READ_WRITE ), recordFormats, chunkSize );
    }

    private StoreChannel channel( String type, OpenMode openMode ) throws IOException
    {
        return fs.open( file( type ), openMode );
    }

    private File file( String type )
    {
        return new File( cacheDirectory, "input-" + type );
    }

    public InputIterable nodes()
    {
        return entities( () -> new InputNodeCacheReader( channel( NODES, READ ), channel( NODES_HEADER, READ ) ) );
    }

    public InputIterable relationships()
    {
        return entities( () -> new InputRelationshipCacheReader( channel( RELATIONSHIPS, READ ), channel( RELATIONSHIPS_HEADER, READ ) ) );
    }

    private InputIterable entities( final ThrowingSupplier<InputIterator, IOException> factory )
    {
        return new InputIterable()
        {
            @Override
            public InputIterator iterator()
            {
                try
                {
                    return factory.get();
                }
                catch ( IOException e )
                {
                    throw new InputException( "Unable to open reader for cached entities", e );
                }
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    @Override
    public void close()
    {
        fs.deleteFile( file( NODES ) );
        fs.deleteFile( file( RELATIONSHIPS ) );
        fs.deleteFile( file( NODES_HEADER ) );
        fs.deleteFile( file( RELATIONSHIPS_HEADER ) );
    }

    static ByteBuffer newChunkHeaderBuffer()
    {
        return ByteBuffer.allocate( Integer.BYTES );
    }
}
