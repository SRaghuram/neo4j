/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import java.io.IOException;

import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;

import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

public abstract class LenientStoreInputChunk implements InputChunk
{
    private final PropertyStore propertyStore;
    private long id;
    private long endId;

    protected final StoreCopyStats stats;
    protected final TokenHolders tokenHolders;
    protected final PageCursor cursor;
    final StoreCopyFilter storeCopyFilter;

    LenientStoreInputChunk( StoreCopyStats stats, PropertyStore propertyStore, TokenHolders tokenHolders, PageCursor cursor, StoreCopyFilter storeCopyFilter )
    {
        this.stats = stats;
        this.propertyStore = propertyStore;
        this.tokenHolders = tokenHolders;
        this.cursor = cursor;
        this.storeCopyFilter = storeCopyFilter;
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
                readAndVisit( id, visitor );
            }
            catch ( Exception e )
            {
                if ( e instanceof InvalidRecordException && e.getMessage().endsWith( "not in use" ) )
                {
                    stats.unused.increment();
                }
                else
                {
                    stats.removed.increment();
                    stats.brokenRecord( recordType(), id, e );
                }
            }
            id++;
            return true;
        }

        return false;
    }

    @Override
    public void close()
    {
        cursor.close();
    }

    abstract void readAndVisit( long id, InputEntityVisitor visitor ) throws IOException;

    abstract String recordType();

    /**
     * Do to the way the visitor work it's important that this method never throws.
     */
    void visitPropertyChainNoThrow( InputEntityVisitor visitor, PrimitiveRecord record )
    {
        try
        {
            if ( record.getNextProp() == Record.NO_NEXT_PROPERTY.intValue() )
            {
                return;
            }

            long nextProp = record.getNextProp();
            var cursorTracer = TRACER_SUPPLIER.get();
            while ( !Record.NO_NEXT_PROPERTY.is( nextProp ) )
            {
                PropertyRecord propertyRecord = propertyStore.getRecord( nextProp, propertyStore.newRecord(), RecordLoad.NORMAL, cursorTracer );
                for ( PropertyBlock propBlock : propertyRecord )
                {
                    propertyStore.ensureHeavy( propBlock, cursorTracer );
                    if ( storeCopyFilter.shouldKeepProperty( propBlock.getKeyIndexId() ) )
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
