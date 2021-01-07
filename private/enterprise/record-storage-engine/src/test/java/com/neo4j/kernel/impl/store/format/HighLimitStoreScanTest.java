/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import com.neo4j.kernel.impl.store.format.highlimit.v306.HighLimitV3_0_6;
import com.neo4j.kernel.impl.store.format.highlimit.v310.HighLimitV3_1_0;
import com.neo4j.kernel.impl.store.format.highlimit.v320.HighLimitV3_2_0;
import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import com.neo4j.kernel.impl.store.format.highlimit.v400.HighLimitV4_0_0;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.internal.id.indexed.IndexedIdGenerator;
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
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
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
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.ArrayUtil.concat;
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
    private DatabaseLayout databaseLayout;

    void startStore( RecordFormats format ) throws IOException
    {
        startStore( format, new DefaultIdGeneratorFactory( directory.getFileSystem(), immediate() ) );
    }

    void startStore( RecordFormats format, IdGeneratorFactory idGeneratorFactory ) throws IOException
    {
        databaseLayout = DatabaseLayout.ofFlat( directory.directory( "db" ) );
        Config config = Config.defaults();
        StoreFactory storeFactory =
                new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, directory.getFileSystem(), format, nullLogProvider(),
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

    @MethodSource( "formatsAndReadVariants" )
    @ParameterizedTest
    void shouldHandleScanningOverSecondaryUnitRecords( RecordFormats format, RecordReadVariant recordReadVariant ) throws IOException
    {
        // given a couple of records, where some require double record units
        startStore( format );
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

    @MethodSource( "formats" )
    @ParameterizedTest
    void shouldHandleScanningOverSecondaryUnitRecordsUsingRelationshipScanCursor( RecordFormats format ) throws IOException
    {
        // given
        startStore( format );
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

    @MethodSource( "formats" )
    @ParameterizedTest
    void shouldHandleScanningOverSecondaryUnitRecordsUsingNodeScanCursor( RecordFormats format ) throws IOException
    {
        // given
        startStore( format );
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

    @MethodSource( "formats" )
    @ParameterizedTest
    void shouldNotPutSecondaryUnitsInFreelistOnIdGeneratorRebuild( RecordFormats format ) throws IOException
    {
        // given
        startStore( format );
        List<NodeRecord> records = createRandomDoubleRecordUnitRecords( this::generateRandomNode, nodeStore, 1_000 );
        LongSet secondaryUnitIds =
                LongSets.immutable.of( records.stream().mapToLong( NodeRecord::getSecondaryUnitId ).filter( id -> !Record.NULL_REFERENCE.is( id ) ).toArray() );

        // when/then
        stopStore();
        directory.getFileSystem().deleteFile( databaseLayout.idNodeStore() );
        MutableBoolean checked = new MutableBoolean();
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( directory.getFileSystem(), immediate() )
        {
            @Override
            public IdGenerator open( PageCache pageCache, Path filename, IdType idType, LongSupplier highIdScanner, long maxId, boolean readOnly,
                    Config config, PageCursorTracer cursorTracer, ImmutableSet<OpenOption> openOptions )
            {
                return new IndexedIdGenerator( pageCache, filename, immediate(), idType, false, highIdScanner, maxId, readOnly, config, cursorTracer,
                        openOptions )
                {
                    @Override
                    public void start( FreeIds freeIdsForRebuild, PageCursorTracer cursorTracer ) throws IOException
                    {
                        // Make sure that no ID we see during rebuild is in our set of secondary unit ID set
                        // We're only checking the node records because that's what we're monitoring
                        if ( filename.equals( databaseLayout.idNodeStore() ) )
                        {
                            checked.setTrue();
                            freeIdsForRebuild.accept( id -> assertThat( secondaryUnitIds.contains( id ) ).isFalse() );
                        }
                        super.start( freeIdsForRebuild, cursorTracer );
                    }
                };
            }

            @Override
            public IdGenerator create( PageCache pageCache, Path filename, IdType idType, long highId, boolean throwIfFileExists, long maxId, boolean readOnly,
                    Config config, PageCursorTracer cursorTracer, ImmutableSet<OpenOption> openOptions )
            {
                throw new UnsupportedOperationException( "Unexpected call since the store should exist" );
            }
        };
        startStore( format, idGeneratorFactory );
        assertThat( checked.booleanValue() ).isTrue();
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
            record.initialize( true, randomId( max ), randomId( max ), randomId( max ), random.nextInt( 1 << 16 ), randomId( max ),
                    randomId( max ), randomId( max ), randomId( max ), false, false );
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
            record.initialize( true, randomId( max ), random.nextBoolean(), randomId( max ), DynamicNodeLabels.dynamicPointer( randomId( max ) ) );
            record.setCreated();
        }
        return record;
    }

    private long randomId( long max )
    {
        long id;
        do
        {
            id = random.nextLong( max );
        }
        while ( IdValidator.isReservedId( id ) );
        return id;
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

    private static Stream<Arguments> formats()
    {
        return Stream.of(
                arguments( HighLimitV3_0_0.RECORD_FORMATS ),
                arguments( HighLimitV3_0_6.RECORD_FORMATS ),
                arguments( HighLimitV3_1_0.RECORD_FORMATS ),
                arguments( HighLimitV3_2_0.RECORD_FORMATS ),
                arguments( HighLimitV3_4_0.RECORD_FORMATS ),
                arguments( HighLimitV4_0_0.RECORD_FORMATS ),
                arguments( HighLimit.RECORD_FORMATS ) );
    }

    private static Stream<Arguments> formatsAndReadVariants()
    {
        List<Arguments> arguments = new ArrayList<>();
        formats().forEach( arg -> Stream.of( RecordReadVariant.values() ).forEach(
                recordReadVariant -> arguments.add( arguments( concat( arg.get(), recordReadVariant ) ) ) ) );
        return arguments.stream();
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
