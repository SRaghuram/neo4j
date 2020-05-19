package org.neo4j.internal.batchimport;

import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.freki.*;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;

import static org.neo4j.internal.freki.Record.deletedRecord;
import static org.neo4j.storageengine.util.IdUpdateListener.IGNORE;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static org.neo4j.internal.helpers.collection.Iterators.asSet;


public class FrekiEntityImporter extends BaseEntityImporter{
    protected final IdMapper idMapper;
    //protected FrekiCommandCreationContext commandCreationContext;
    protected Collection<StorageCommand> commands = new ArrayList<>();
    protected Set<StorageProperty> propsAdd = null;
    PageCursor bigPropertyCursor;
    private DenseRelationshipsWorkSync.Batch denseRelationshipUpdates;
    private DenseRelationshipsWorkSync denseRelationshipsWorkSync;
    protected FrekiBatchStores frekiBatchStores;
    MemoryTracker memoryTracker;

    public FrekiEntityImporter(BatchingStoreBase basicNeoStore, IdMapper idMapper, PropertyValueLookup inputIdLookup,
                               DataImporter.Monitor monitor, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker) {
        super(basicNeoStore, idMapper, inputIdLookup, monitor, pageCacheTracer);
        frekiBatchStores = (FrekiBatchStores) basicNeoStore;
        this.idMapper = idMapper;
        this.memoryTracker = memoryTracker;
        //commandCreationContext = new FrekiCommandCreationContext(frekiBatchStores.stores, basicNeoStore.getIdGeneratorFactory(), cursorTracer);
        this.denseRelationshipsWorkSync = new DenseRelationshipsWorkSync( frekiBatchStores.stores.denseStore );

        try {
            bigPropertyCursor = ((BigPropertyValueStore)frekiBatchStores.stores.bigPropertyValueStore).openWriteCursor( cursorTracer);
        } catch (IOException io)
        {
            throw new UnderlyingStorageException( io.getMessage());
        }
    }

    @Override
    public void freeUnusedIds() {

    }
    @Override
    public boolean property( String key, Object value){
        return property( key, value, null);
    }

    @Override
    public boolean property( String key, Object value, Object strValue )
    {
        super.property(key, value);
        return property( baseNeoStore.getPropertyKeyRepository().getOrCreateId( key ), value );
    }
    @Override
    public boolean property( int propertyKeyId, Object value )
    {
        //save value of property here
        super.property(propertyKeyId, value);
        propsAdd = asSet( new PropertyKeyValue( propertyKeyId, stringValue((String)value)) );
        return true;
    }

    @Override
    public boolean propertyId( long nextProp )
    {
        super.propertyId( nextProp );
        return true;
    }

    public void endOfEntity()
    {
        try {
            for (StorageCommand command : commands) {
                if (command instanceof FrekiCommand.SparseNode) {
                    for ( FrekiCommand.RecordChange change : (FrekiCommand.SparseNode)command )
                    {
                        int sizeExp = change.sizeExp();
                        SimpleStore store = frekiBatchStores.stores.mainStore( sizeExp );
                        try ( PageCursor cursor = store.openWriteCursor( cursorTracer ) )
                        {
                            Record afterRecord = change.after != null ? change.after : deletedRecord( sizeExp, change.recordId() );
                            store.write( cursor, afterRecord, IGNORE, cursorTracer );
                        }
                    }
                } else if (command instanceof FrekiCommand.BigPropertyValue) {
                    ((BigPropertyValueStore) frekiBatchStores.stores.bigPropertyValueStore).write(bigPropertyCursor,
                            ByteBuffer.wrap(((FrekiCommand.BigPropertyValue) command).bytes, 0, ((FrekiCommand.BigPropertyValue) command).length),
                            ((FrekiCommand.BigPropertyValue) command).pointer);
                } else if (command instanceof FrekiCommand.DenseNode) {
                    if (denseRelationshipUpdates == null) {
                        denseRelationshipUpdates = denseRelationshipsWorkSync.newBatch();
                    }
                    denseRelationshipUpdates.add(((FrekiCommand.DenseNode) command));
                }
                else
                    System.out.println("Unknown command:["+command.toString()+"]");
            }
            if (denseRelationshipUpdates != null)
            {
                denseRelationshipUpdates.applyAsync( pageCacheTracer );
                denseRelationshipUpdates = null;
            }
        } catch (IOException io)
        {
            throw new UnderlyingStorageException( io.getMessage());
        }
        super.endOfEntity();
    }
    @Override
    public void close()
    {
        bigPropertyCursor.close();
        //commandCreationContext.close();
        super.close();
    }
}
