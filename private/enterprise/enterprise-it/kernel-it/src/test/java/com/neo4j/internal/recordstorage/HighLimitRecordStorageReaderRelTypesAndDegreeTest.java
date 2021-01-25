/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.recordstorage;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.recordstorage.RecordStorageReaderRelTypesAndDegreeTest;
import org.neo4j.internal.recordstorage.TestRelType;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.util.SingleDegree;
import org.neo4j.test.rule.RecordStorageEngineRule;

import static com.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.HEADER_BIT_FIXED_REFERENCE;
import static com.neo4j.kernel.impl.store.format.highlimit.RelationshipGroupRecordFormat.HAS_INCOMING_BIT;
import static com.neo4j.kernel.impl.store.format.highlimit.RelationshipGroupRecordFormat.HAS_LOOP_BIT;
import static com.neo4j.kernel.impl.store.format.highlimit.RelationshipGroupRecordFormat.HAS_NEXT_BIT;
import static com.neo4j.kernel.impl.store.format.highlimit.RelationshipGroupRecordFormat.HAS_OUTGOING_BIT;
import static com.neo4j.kernel.impl.store.format.highlimit.RelationshipRecordFormat.HAS_FIRST_CHAIN_NEXT_BIT;
import static com.neo4j.kernel.impl.store.format.highlimit.RelationshipRecordFormat.HAS_PROPERTY_BIT;
import static com.neo4j.kernel.impl.store.format.highlimit.RelationshipRecordFormat.HAS_SECOND_CHAIN_NEXT_BIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.ANY_PAGE_SIZE;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

public class HighLimitRecordStorageReaderRelTypesAndDegreeTest extends RecordStorageReaderRelTypesAndDegreeTest
{
    @Override
    protected RecordStorageEngineRule.Builder modify( RecordStorageEngineRule.Builder builder )
    {
        builder = super.modify( builder );
        builder = builder.setting( GraphDatabaseSettings.record_format, HighLimit.NAME );
        return builder;
    }

    @Test
    void degreesForDenseNodeWithPartiallyDeletedRelGroupChainMustThrowOnDecodingErrors() throws Exception
    {
        NeoStores neoStores = resolveNeoStores();
        RelationshipGroupStore groupStore = neoStores.getRelationshipGroupStore();
        long nodeId;

        do
        {
            nodeId = createNode( randomRelCount(), randomRelCount(), randomRelCount() );
        }
        while ( groupStore.getHighId() < groupStore.getRecordsPerPage() );

        // Sabotage the relationship group at the end of the first page:
        // First, mark it as not-in-use.
        int groupId = groupStore.getRecordsPerPage() - 1;
        RelationshipGroupRecord record = getRelGroupRecord( groupId );
        TestRelType type = relTypeForId( record.getType() );
        markRelGroupNotInUse( nodeId, type );
        // Second, corrupt it such that decoding it will throw out-of-bounds.
        try ( PagedFile pagedFile = pageCache.map( groupStore.getStorageFile(), 0, Sets.immutable.of( ANY_PAGE_SIZE ) );
              PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, NULL ) )
        {
            assertTrue( cursor.next() );
            int offset = groupStore.getRecordSize() * groupId;
            cursor.setOffset( offset );
            int header = cursor.getByte( offset );
            header |= HEADER_BIT_FIXED_REFERENCE; // Enable the dynamic record format.
            header |= HAS_OUTGOING_BIT;
            header |= HAS_INCOMING_BIT;
            header |= HAS_LOOP_BIT;
            header |= HAS_NEXT_BIT;
            cursor.putByte( (byte) header );
            while ( cursor.getOffset() < cursor.getCurrentPageSize() - 1 )
            {
                // Fill data with a byte that'll maximise the space used by the compressed references.
                cursor.putByte( (byte) 0xF8 );
            }
        }

        // Verify that an exception is thrown when we attempt to get the node degrees and thus decode all group records.
        StorageNodeCursor cursor = newCursor( nodeId );
        var e = assertThrows( UnderlyingStorageException.class, () -> cursor.degrees( ALL_RELATIONSHIPS, new SingleDegree(), true ) );
        assertThat( e ).hasMessageContaining( "out of bounds" );
    }

    @Test
    void degreesForDenseNodeWithPartiallyDeletedRelChainMustThrowOnDecodingErrors() throws Exception
    {
        NeoStores neoStores = resolveNeoStores();
        RelationshipStore relStore = neoStores.getRelationshipStore();
        long nodeId;

        do
        {
            nodeId = createNode( randomRelCount(), randomRelCount(), randomRelCount() );
        }
        while ( relStore.getHighId() < relStore.getRecordsPerPage() );

        // Relationship at the end of the page. That's the one we want to sabotage.
        int relId = relStore.getRecordsPerPage() - 1;

        // Make sure a relationship group points to the sabotaged relationship as its first relationship.
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        RelationshipGroupRecord relGroupRecord = getRelGroupRecord( nodeRecord.getNextRel() );
        relGroupRecord.setFirstOut( relId );
        update( relGroupRecord );

        // Mark it as not-in-use, and corrupt it such that decoding it will throw out-of-bounds.
        try ( PagedFile pagedFile = pageCache.map( relStore.getStorageFile(), 0, Sets.immutable.of( ANY_PAGE_SIZE ) );
              PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, NULL ) )
        {
            assertTrue( cursor.next() );
            int offset = relStore.getRecordSize() * relId;
            cursor.setOffset( offset );
            int header = cursor.getByte( offset );
            header |= HEADER_BIT_FIXED_REFERENCE; // Enable the dynamic record format.
            header |= HAS_PROPERTY_BIT;
            header |= HAS_FIRST_CHAIN_NEXT_BIT;
            header |= HAS_SECOND_CHAIN_NEXT_BIT;
            cursor.putByte( (byte) header );
            while ( cursor.getOffset() < cursor.getCurrentPageSize() - 1 )
            {
                // Fill data with a byte that'll maximise the space used by the compressed references.
                cursor.putByte( (byte) 0xF8 );
            }
        }

        // Verify that an exception is thrown when we attempt to get the node degrees and thus decode all group records.
        StorageNodeCursor cursor = newCursor( nodeId );
        var e = assertThrows( UnderlyingStorageException.class, () -> cursor.degrees( ALL_RELATIONSHIPS, new SingleDegree(), true ) );
        assertThat( e ).hasMessageContaining( "out of bounds" );
    }
}
