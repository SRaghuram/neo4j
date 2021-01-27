/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import java.io.IOException;

import org.neo4j.consistency.RecordType;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;

import static org.neo4j.io.IOUtils.closeAllUnchecked;

public abstract class LenientStoreInputChunk implements InputChunk
{
    private static final String COPY_STORE_READER_TAG = "copyStoreReader";
    private final PropertyStore propertyStore;
    private long id;
    private long endId;

    protected final StoreCopyStats stats;
    protected final TokenHolders tokenHolders;
    protected final PageCursor cursor;
    final StoreCopyFilter storeCopyFilter;
    private final PageCursorTracer cursorTracer;
    private final MutableLongSet seenPropertyRecordIds = LongSets.mutable.empty();
    private final PageCursor propertyCursor;
    private final PropertyRecord propertyRecord;

    LenientStoreInputChunk( StoreCopyStats stats, PropertyStore propertyStore, TokenHolders tokenHolders, StoreCopyFilter storeCopyFilter,
            CommonAbstractStore<?,?> store, PageCacheTracer pageCacheTracer )
    {
        this.stats = stats;
        this.propertyStore = propertyStore;
        this.tokenHolders = tokenHolders;
        this.storeCopyFilter = storeCopyFilter;
        this.cursorTracer = pageCacheTracer.createPageCursorTracer( COPY_STORE_READER_TAG );
        this.cursor = store.openPageCursorForReading( 0, cursorTracer );
        this.propertyCursor = propertyStore.openPageCursorForReading( 0, cursorTracer );
        this.propertyRecord = propertyStore.newRecord();
    }

    void setChunkRange( long startId, long endId )
    {
        this.id = startId;
        this.endId = endId;
    }

    @Override
    public boolean next( InputEntityVisitor visitor )
    {
        if ( id < endId )
        {
            stats.count.increment();
            try
            {
                readAndVisit( id, visitor, cursorTracer );
            }
            catch ( Exception e )
            {
                stats.removed.increment();
                stats.brokenRecord( recordType(), id, e );
            }
            id++;
            return true;
        }

        return false;
    }

    @Override
    public void close()
    {
        closeAllUnchecked( cursor, propertyCursor, cursorTracer );
    }

    abstract void readAndVisit( long id, InputEntityVisitor visitor, PageCursorTracer cursorTracer ) throws IOException;

    abstract String recordType();

    /**
     * Do to the way the visitor work it's important that this method never throws.
     */
    void visitPropertyChainNoThrow( InputEntityVisitor visitor, PrimitiveRecord record, RecordType owningEntityType, long[] owningEntityTokens )
    {
        try
        {
            if ( record.getNextProp() == Record.NO_NEXT_PROPERTY.intValue() )
            {
                return;
            }

            // We're detecting property record chain loops in this method, so prepare the set by clearing it
            seenPropertyRecordIds.clear();

            long nextProp = record.getNextProp();
            while ( !Record.NO_NEXT_PROPERTY.is( nextProp ) )
            {
                if ( !seenPropertyRecordIds.add( nextProp ) )
                {
                    stats.circularPropertyChain( recordType(), record, propertyRecord );
                    return;
                }

                propertyStore.getRecordByCursor( nextProp, propertyRecord, RecordLoad.NORMAL, propertyCursor );
                for ( PropertyBlock propBlock : propertyRecord )
                {
                    propertyStore.ensureHeavy( propBlock, cursorTracer );
                    if ( storeCopyFilter.shouldKeepProperty( propBlock.getKeyIndexId(), owningEntityType, owningEntityTokens ) )
                    {
                        try
                        {
                            String key = tokenHolders.propertyKeyTokens().getTokenById( propBlock.getKeyIndexId() ).name();
                            Value propertyValue = propBlock.newPropertyValue( propertyStore, cursorTracer );

                            visitor.property( key, propertyValue.asObject() );
                        }
                        catch ( TokenNotFoundException ignored )
                        {
                            stats.brokenPropertyToken( recordType(), record, propBlock.newPropertyValue( propertyStore, cursorTracer ),
                                    propBlock.getKeyIndexId() );
                        }
                    }
                }
                nextProp = propertyRecord.getNextProp();
            }
        }
        catch ( Exception e )
        {
            stats.brokenPropertyChain( recordType(), record, e );
        }
    }
}
