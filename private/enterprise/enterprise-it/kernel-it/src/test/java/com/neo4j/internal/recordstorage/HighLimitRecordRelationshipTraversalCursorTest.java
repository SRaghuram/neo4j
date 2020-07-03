/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.recordstorage;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.recordstorage.RecordRelationshipTraversalCursorTest;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

class HighLimitRecordRelationshipTraversalCursorTest extends RecordRelationshipTraversalCursorTest
{
    @Override
    protected RecordFormats getRecordFormats()
    {
        return HighLimit.RECORD_FORMATS;
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void mustThrowOnOutOfBoundsWhenRetrievingChainWithUnusedLink( boolean dense ) throws IOException
    {
        RelationshipStore store = neoStores.getRelationshipStore();
        int recordsPerPage = store.getRecordsPerPage();
        createRelationshipStructure( dense, homogenousRelationships( recordsPerPage, TYPE1, RelationshipDirection.LOOP ) );
        int maxRecordId = recordsPerPage - 1;
        store.setHighestPossibleIdInUse( maxRecordId );
        Path storageFile = store.getStorageFile();
        unUseRecord( maxRecordId );

        // Sabotage the last record in the page, so that reading it will go out of bounds on the page.
        try ( PagedFile pagedFile = pageCache.map( storageFile, 0, Sets.immutable.of( PageCacheOpenOptions.ANY_PAGE_SIZE ) );
              PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, PageCursorTracer.NULL ) )
        {
            int offset = store.getRecordSize() * maxRecordId;
            assertTrue( cursor.next() );
            cursor.setOffset( offset + 1 );
            do
            {
                cursor.putByte( (byte) 0xF8 );
            }
            while ( cursor.getOffset() < cursor.getCurrentPageSize() - 1 );
        }

        int[] expectedRelationshipIds = new int[maxRecordId - 1];
        for ( int i = 0; i < expectedRelationshipIds.length; i++ )
        {
            expectedRelationshipIds[i] = i + 1;
        }
        try ( StorageRelationshipTraversalCursor cursor = getNodeRelationshipCursor() )
        {
            cursor.init( FIRST_OWNING_NODE, 1, selection( TYPE1, Direction.OUTGOING ) );
            for ( int expectedRelationshipId : expectedRelationshipIds )
            {
                assertTrue( cursor.next() );
                assertEquals( expectedRelationshipId, cursor.entityReference(), "Should load next relationship in a sequence" );
            }
            var e = assertThrows( UnderlyingStorageException.class, cursor::next );
            assertThat( e ).hasMessageContaining( "out of bounds" );
        }
    }
}
