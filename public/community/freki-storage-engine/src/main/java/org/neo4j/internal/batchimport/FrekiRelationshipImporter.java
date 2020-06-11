package org.neo4j.internal.batchimport;

import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.freki.*;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.storageengine.api.StorageEntityScanCursor.NO_ID;

public class FrekiRelationshipImporter extends FrekiEntityImporter{
    private final DataStatistics.Client typeCounts;
    private Object startId, endId;
    private long startNodeId = NO_ID, endNodeId = NO_ID, relationshipId = NO_ID;
    private Group startIdGroup, endIdGroup;
    private String type = null;
    private int typeId;
    protected Collector badCollector;
    private long relationshipCount;
    public FrekiRelationshipImporter(BatchingStoreBase basicNeoStore, IdMapper idMapper, DataStatistics typeDistribution, DataImporterMonitor monitor, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker, Collector badCollector) {
        super(basicNeoStore, idMapper, null, monitor, pageCacheTracer,  memoryTracker);
        this.badCollector = badCollector;
        this.typeCounts = typeDistribution.newClient();
    }
    @Override
    public boolean startId( long id )
    {
        return true;
    }

    @Override
    public boolean startId( Object id, Group group )
    {
        this.startId = id;
        this.startIdGroup = group;

        startNodeId = getNodeId( id, group );
        try {
            FrekiCommandCreationContext commandCreationContext = new FrekiCommandCreationContext(frekiBatchStores.stores, baseNeoStore.getIdGeneratorFactory(), cursorTracer, memoryTracker);
            synchronized (baseNeoStore )
            {
                relationshipId = commandCreationContext.reserveRelationship(startNodeId);
            }
        } catch (NullPointerException ne)
        {
            System.out.print("4");
            badCollector.collectBadRelationship( startId, group( startIdGroup ).name(), type, endId, group( endIdGroup ).name(),
                    1 == IdMapper.ID_NOT_FOUND ? startId : endId );
        }catch (Exception ne)
        {
            System.out.print("5");
            badCollector.collectBadRelationship( startId, group( startIdGroup ).name(), type, endId, group( endIdGroup ).name(),
                    1 == IdMapper.ID_NOT_FOUND ? startId : endId );
        }
        return true;
    }

    @Override
    public boolean endId( long id )
    {
        return true;
    }

    @Override
    public boolean endId( Object id, Group group )
    {
        this.endId = id;
        this.endIdGroup = group;
        endNodeId = getNodeId( id, group );
        return true;
    }

    @Override
    public boolean type( int typeId )
    {
        return true;
    }

    @Override
    public boolean type( String type )
    {
        this.type = type;
        return true;
    }

    @Override
    public void endOfEntity()
    {
        if (startNodeId == NO_ID || endNodeId == NO_ID)
            badCollector.collectBadRelationship( startId, group( startIdGroup ).name(), type, endId, group( endIdGroup ).name(),
                                1 == IdMapper.ID_NOT_FOUND ? startId : endId );
        else {
                if (type != null) {
                    typeId = baseNeoStore.getRelationshipTypeRepository().getOrCreateId(type);
                }
                try
                {
                    //commands.clear();
                    CommandCreator commandCreator = new CommandCreator(commands, frekiBatchStores.getStores() , null, cursorTracer, memoryTracker);
                    commandCreator.visitCreatedRelationship(relationshipId, typeId, startNodeId, endNodeId, propsAdd);
                    relationshipCount++;
                    typeCounts.increment( typeId );
                    commandCreator.close();
                    if (commands.size() >5000) {
                        super.endOfEntity();
                        commands.clear();
                    }
                } catch (UnsupportedOperationException ue) {
                    System.out.print("1");
                    badCollector.collectBadRelationship(startId, group(startIdGroup).name(), type, endId, group(endIdGroup).name(),
                            1 == IdMapper.ID_NOT_FOUND ? startId : endId);
                } catch (NullPointerException ne) {
                    System.out.print("2");
                    badCollector.collectBadRelationship(startId, group(startIdGroup).name(), type, endId, group(endIdGroup).name(),
                            1 == IdMapper.ID_NOT_FOUND ? startId : endId);
                } catch (Exception e) {
                    System.out.println("3-"+e.getMessage());
                    badCollector.collectBadRelationship(startId, group(startIdGroup).name(), type, endId, group(endIdGroup).name(),
                            1 == IdMapper.ID_NOT_FOUND ? startId : endId);
                }
        }
        //reset values
        entityPropertyCount = 0;
        startId = endId = null;
        startIdGroup = endIdGroup = null;
        type = null;
        startNodeId = endNodeId = NO_ID;
        typeId = -1;
    }
    private Group group( Group group )
    {
        return group != null ? group : Group.GLOBAL;
    }
    @Override
    public void close()
    {
        super.close();
        typeCounts.close();
        monitor.relationshipsImported( relationshipCount );
        cursorTracer.close();
    }
}
