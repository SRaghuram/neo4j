/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongFunction;

import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.recordstorage.RecordNodeCursor;
import org.neo4j.internal.recordstorage.RecordRelationshipScanCursor;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.io.IOUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.IdUpdateListener;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

@EphemeralPageCacheExtension
@ExtendWith( RandomExtension.class )
class HighLimitStoreScanTest
{
    // Somewhat optional, but we're running this test w/o any sort of context, like buffered IDs or transaction management
    // and therefore the "freeing"  of IDs has to be managed by us manually if we're to get some ID reuse.
    private static final IdUpdateListener FREE_DELETED_IMMEDIATELY = new IdUpdateListener()
    {
        @Override
        public void close()
        {
        }

        @Override
        public void markIdAsUsed( IdType idType, IdGenerator idGenerator, long id, PageCursorTracer cursorTracer )
        {
            try ( IdGenerator.Marker marker = idGenerator.marker( cursorTracer ) )
            {
                marker.markUsed( id );
            }
        }

        @Override
        public void markIdAsUnused( IdType idType, IdGenerator idGenerator, long id, PageCursorTracer cursorTracer )
        {
            try ( IdGenerator.Marker marker = idGenerator.marker( cursorTracer ) )
            {
                marker.markDeleted( id );
                marker.markFree( id ); // <-- this is the thing that is the manual freeing that we have to do
            }
        }
    };

    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    @Inject
    private RandomRule random;

    private NeoStores neoStores;
    private RelationshipStore relationshipStore;
    private NodeStore nodeStore;

    @BeforeEach
    void startStore() throws IOException
    {
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( directory.directory( "db" ) );
        Config config = Config.defaults();
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( directory.getFileSystem(), RecoveryCleanupWorkCollector.immediate() );
        StoreFactory storeFactory =
                new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, directory.getFileSystem(), HighLimit.RECORD_FORMATS, nullLogProvider(),
                        PageCacheTracer.NULL, Sets.immutable.empty() );
        neoStores = storeFactory.openAllNeoStores( true );
        neoStores.start( NULL );
        relationshipStore = neoStores.getRelationshipStore();
        nodeStore = neoStores.getNodeStore();
    }

    @AfterEach
    void stopStore()
    {
        neoStores.close();
    }

    @EnumSource( RecordReadVariant.class )
    @ParameterizedTest
    void shouldHandleScanningOverSecondaryUnitRecords( RecordReadVariant recordReadVariant )
    {
        // given a couple of records, where some require double record units
        List<RelationshipRecord> records = createRandomDoubleRecordUnitRecords( this::generateRandomRelationship, relationshipStore, 1_000 );

        // when opening a cursor for the purpose of scanning all ids
        // then the cursor should be able to see all records and ignore those ids that are secondary units (verify fail on non-scan too)
        RecordLoad[] modes = new RecordLoad[]{RecordLoad.CHECK, RecordLoad.NORMAL, RecordLoad.ALWAYS, RecordLoad.FORCE};
        for ( RecordLoad mode : modes )
        {
            if ( mode != RecordLoad.FORCE )
            {
                // All except FORCE mode should eventually fail on a scan
                assertThrows( InvalidRecordException.class, () -> scanAndVerifyAllRecords( records, mode, recordReadVariant ) );
            }
            // No scan-variant mode should fail, but be able to complete a scan
            scanAndVerifyAllRecords( records, mode.lenient(), recordReadVariant );
        }
    }

    @Test
    void shouldHandleScanningOverSecondaryUnitRecordsUsingRelationshipScanCursor()
    {
        // given
        List<RelationshipRecord> records = createRandomDoubleRecordUnitRecords( this::generateRandomRelationship, relationshipStore, 1_000 );

        // when/then
        RecordStorageReader reader = new RecordStorageReader( neoStores );
        try ( RecordRelationshipScanCursor scanCursor = reader.allocateRelationshipScanCursor( NULL ) )
        {
            scanCursor.scan();
            Iterator<RelationshipRecord> expectedRelationships = records.iterator();
            while ( scanCursor.next() )
            {
                assertThat( expectedRelationships ).hasNext();
                assertEquals( expectedRelationships.next(), copyToRecord( scanCursor ) );
            }
            assertThat( expectedRelationships.hasNext() ).isFalse();
        }
    }

    @Test
    void shouldHandleScanningOverSecondaryUnitRecordsUsingNodeScanCursor()
    {
        // given
        List<NodeRecord> records = createRandomDoubleRecordUnitRecords( this::generateRandomNode, nodeStore, 1_000 );

        // when/then
        RecordStorageReader reader = new RecordStorageReader( neoStores );
        try ( RecordNodeCursor scanCursor = reader.allocateNodeCursor( NULL ) )
        {
            scanCursor.scan();
            Iterator<NodeRecord> expectedRelationships = records.iterator();
            while ( scanCursor.next() )
            {
                assertThat( expectedRelationships ).hasNext();
                assertEquals( expectedRelationships.next(), copyToRecord( scanCursor ) );
            }
            assertThat( expectedRelationships.hasNext() ).isFalse();
        }
    }

    private <T extends AbstractBaseRecord> List<T> createRandomDoubleRecordUnitRecords( LongFunction<T> generator, RecordStore<T> store, int count )
    {
        List<T> records = new ArrayList<>();
        createRandomRecords( records, generator, store, count );
        // delete some and then recreate some more, this to get some more randomness in the locations of primary vs secondary ID
        // otherwise they'll end up next to each other always
        deleteRandomRecords( records, store, records.size() / 2 );
        createRandomRecords( records, generator, store, count / 2 );
        records.sort( Comparator.comparing( AbstractBaseRecord::getId ) );
        return records;
    }

    private void scanAndVerifyAllRecords( List<RelationshipRecord> records, RecordLoad mode, RecordReadVariant recordReadVariant )
    {
        Iterator<RelationshipRecord> expectedRecords = records.iterator();
        long startId = relationshipStore.getNumberOfReservedLowIds();
        long highId = relationshipStore.getHighId();
        PageCursor cursor = recordReadVariant.openCursor( relationshipStore );
        try
        {
            for ( long id = startId; id < highId; id++ )
            {
                RelationshipRecord record = relationshipStore.newRecord();
                // Even tho it's probably not a good thing to do a scan with NORMAL mode (which says that all IDs need to be in use),
                // let's test it and just compensate for that in this test so that it can ignore those, even if reading unused records throws exception
                try
                {
                    recordReadVariant.readRecord( relationshipStore, record, id, mode, cursor );
                }
                catch ( InvalidRecordException e )
                {
                    // Pity that we have to check exception message, it would have been better with a specific sub-exception actually
                    if ( !e.getMessage().contains( "not in use" ) )
                    {
                        throw e;
                    }
                    // This is OK, it's just the NORMAL mode(s) being picky about which records it can read
                    // Catching this exception here
                    assertThat( mode ).isIn( RecordLoad.NORMAL, RecordLoad.LENIENT_NORMAL );
                }
                if ( record.inUse() )
                {
                    assertThat( expectedRecords ).hasNext();
                    RelationshipRecord expectedRecord = expectedRecords.next();
                    assertThat( record ).isEqualTo( expectedRecord );
                }
            }
        }
        finally
        {
            IOUtils.closeAllUnchecked( cursor );
        }
        assertThat( expectedRecords.hasNext() ).isFalse();
    }

    private <T extends AbstractBaseRecord> void deleteRandomRecords( List<T> records, RecordStore<T> store, int count )
    {
        for ( int i = 0; i < count; i++ )
        {
            T record = random.among( records );
            records.remove( record );
            record.setInUse( false );
            store.updateRecord( record, FREE_DELETED_IMMEDIATELY, NULL );
        }
    }

    private <T extends AbstractBaseRecord> void createRandomRecords( List<T> records, LongFunction<T> generator, RecordStore<T> store, int count )
    {
        int numDoubleRecords = 0;
        for ( int i = 0; i < count || numDoubleRecords < 10; i++ )
        {
            T record = generator.apply( store.nextId( NULL ) );
            store.prepareForCommit( record, NULL );
            if ( record.requiresSecondaryUnit() )
            {
                numDoubleRecords++;
            }
            if ( record.inUse() )
            {
                store.updateRecord( record, NULL );
                records.add( record );
            }
        }
    }

    private RelationshipRecord generateRandomRelationship( long id )
    {
        RelationshipRecord record = new RelationshipRecord( id );
        boolean inUse = random.nextBoolean();
        if ( inUse )
        {
            boolean shouldRequireSecondaryUnit = random.nextInt( 4 ) == 0;
            long max = shouldRequireSecondaryUnit ? 1L << 50 : 1L << 32;
            record.initialize( true, random.nextLong( max ), random.nextLong( max ), random.nextLong( max ), random.nextInt( 1 << 16 ), random.nextLong( max ),
                    random.nextLong( max ), random.nextLong( max ), random.nextLong( max ), false, false );
            record.setCreated();
        }
        return record;
    }

    private NodeRecord generateRandomNode( long id )
    {
        NodeRecord record = new NodeRecord( id );
        boolean inUse = random.nextBoolean();
        if ( inUse )
        {
            boolean shouldRequireSecondaryUnit = random.nextInt( 4 ) == 0;
            long max = shouldRequireSecondaryUnit ? 1L << 50 : 1L << 32;
            record.initialize( true, random.nextLong( max ), random.nextBoolean(), random.nextLong( max ),
                    DynamicNodeLabels.dynamicPointer( random.nextLong( max ) ) );
            record.setCreated();
        }
        return record;
    }

    private NodeRecord copyToRecord( NodeRecord cursor )
    {
        NodeRecord readNode = nodeStore.newRecord();
        readNode.initialize( cursor.inUse(), cursor.getNextProp(), cursor.isDense(), cursor.getNextRel(), cursor.getLabelField() );
        readNode.setLabelField( cursor.getLabelField(), new ArrayList<>( cursor.getDynamicLabelRecords() ) );
        copyAbstractRecordData( cursor, readNode );
        return readNode;
    }

    private RelationshipRecord copyToRecord( RecordRelationshipScanCursor cursor )
    {
        RelationshipRecord readRelationship = relationshipStore.newRecord();
        readRelationship.initialize( cursor.inUse(), cursor.getNextProp(), cursor.getFirstNode(), cursor.getSecondNode(),
                cursor.type(), cursor.getFirstPrevRel(), cursor.getFirstNextRel(), cursor.getSecondPrevRel(),
                cursor.getSecondNextRel(), cursor.isFirstInFirstChain(), cursor.isFirstInSecondChain() );
        copyAbstractRecordData( cursor, readRelationship );
        return readRelationship;
    }

    private static void copyAbstractRecordData( AbstractBaseRecord from, AbstractBaseRecord to )
    {
        to.setId( from.getId() );
        to.setRequiresSecondaryUnit( from.requiresSecondaryUnit() );
        to.setSecondaryUnitIdOnLoad( from.getSecondaryUnitId() );
        to.setUseFixedReferences( from.isUseFixedReferences() );
    }

    enum RecordReadVariant
    {
        GET_RECORD_BY_CURSOR
                {
                    @Override
                    PageCursor openCursor( RelationshipStore store )
                    {
                        return store.openPageCursorForReading( 0, NULL );
                    }

                    @Override
                    void readRecord( RelationshipStore store, RelationshipRecord record, long id, RecordLoad mode, PageCursor cursor )
                    {
                        store.getRecordByCursor( id, record, mode, cursor );
                    }
                },
        GET_RECORD_BY_CURSOR_WITH_PREFETCHING
                {
                    @Override
                    PageCursor openCursor( RelationshipStore store )
                    {
                        return store.openPageCursorForReadingWithPrefetching( 0, NULL );
                    }

                    @Override
                    void readRecord( RelationshipStore store, RelationshipRecord record, long id, RecordLoad mode, PageCursor cursor )
                    {
                        store.getRecordByCursor( id, record, mode, cursor );
                    }
                },
        GET_RECORD
                {
                    @Override
                    PageCursor openCursor( RelationshipStore store )
                    {
                        return null;
                    }

                    @Override
                    void readRecord( RelationshipStore store, RelationshipRecord record, long id, RecordLoad mode, PageCursor cursor )
                    {
                        store.getRecord( id, record, mode, NULL );
                    }
                };

        abstract PageCursor openCursor( RelationshipStore store );

        abstract void readRecord( RelationshipStore store, RelationshipRecord record, long id, RecordLoad mode, PageCursor cursor );
    }
}
