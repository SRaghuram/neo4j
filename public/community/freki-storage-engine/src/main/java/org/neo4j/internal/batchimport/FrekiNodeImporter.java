package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.freki.*;
import org.neo4j.internal.id.IdType;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.memory.MemoryTracker;

import java.util.stream.LongStream;

import static java.util.Arrays.copyOf;
import static java.util.Collections.emptyList;

import static org.neo4j.storageengine.api.StorageEntityScanCursor.NO_ID;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

public class FrekiNodeImporter extends FrekiEntityImporter{
    long startId = 0, currentId = NO_ID;
    private BatchingIdGetter nodeIds;
    private boolean hasLabelField;
    private int labelsCursor;
    private String[] labels = new String[10];
    private long nodeId = NO_ID;

    public FrekiNodeImporter(BatchingStoreBase basicNeoStore, IdMapper idMapper, PropertyValueLookup inputIdLookup, DataImporter.Monitor monitor, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker) {
        super(basicNeoStore, idMapper, inputIdLookup, monitor, pageCacheTracer, memoryTracker);
        this.nodeIds = new BatchingIdGetter( basicNeoStore.getIdGeneratorFactory().get( IdType.NODE ), (int)basicNeoStore.getNodeRecordSize() );
    }

    @Override
    public boolean id( Object id, Group group )
    {
        nodeId = nodeIds.nextId( cursorTracer );
        putIdInIdMapper( id, nodeId, group );
        return true;
    }

    @Override
    public boolean labels( String[] labels )
    {
        assert !hasLabelField;
        int requiredLength = labelsCursor + labels.length;
        if ( requiredLength > this.labels.length )
        {
            this.labels = copyOf( this.labels, Integer.max( requiredLength, this.labels.length * 2 ) );
        }
        System.arraycopy( labels, 0, this.labels, labelsCursor, labels.length );
        labelsCursor += labels.length;

        return true;
    }

    @Override
    public boolean labelField( long labelField )
    {
        hasLabelField = true;
        return true;
    }


    @Override
    public void endOfEntity()
    {
        commands.clear();
        try {
            CommandCreator commandCreator = new CommandCreator(commands, ((FrekiBatchStores)baseNeoStore).stores, null, cursorTracer, memoryTracker);
            commands.add(new FrekiCommand.NodeCount(ANY_LABEL, (long) 1));
            commandCreator.visitCreatedNode(nodeId);
            //add properties
            commandCreator.visitNodePropertyChanges(nodeId, propsAdd, emptyList(), IntSets.immutable.empty());
            // Compose the labels
            long[] labelIds = null;
            if (!hasLabelField) {
                labelIds = baseNeoStore.getLabelRepository().getOrCreateIds(labels, labelsCursor);
                MutableLongSet addedLabels = LongSets.mutable.empty();
                LongStream.of(labelIds).forEach(addedLabels::add);
                commandCreator.visitNodeLabelChanges(nodeId, addedLabels, LongSets.immutable.empty());
                for (long labelId : labelIds)
                    commands.add(new FrekiCommand.NodeCount((int) labelId, (long) 1));
            }
            labelsCursor = 0;
            hasLabelField = false;

            super.endOfEntity( commandCreator );
        } catch (Exception ke)
        {
            System.out.println("CommandCrreator error-"+ke.getMessage());
        }
    }
    @Override
    public void close()
    {
        super.close();
    }
}
