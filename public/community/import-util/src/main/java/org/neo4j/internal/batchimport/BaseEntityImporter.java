package org.neo4j.internal.batchimport;

import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.cache.idmapping.reverseIdMap.NodeInputIdPropertyLookup;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.kernel.impl.store.StoreTokens;
import org.neo4j.token.TokenHolders;

public abstract class BaseEntityImporter implements InputEntityVisitor
{
    private static final String ENTITY_IMPORTER_TAG = "entityImporter";
    protected final IdMapper idMapper;
    protected  final PropertyValueLookup inputIdLookup;
    protected final PageCursorTracer cursorTracer;
    protected final DataImporterMonitor monitor;
    private long propertyCount;
    protected int entityPropertyCount; // just for the current entity
    protected boolean hasPropertyId;
    protected long propertyId;
    BatchingStoreBase baseNeoStore;
    protected TokenHolders tokenHolders;
    protected PageCacheTracer pageCacheTracer;

    public BaseEntityImporter(BatchingStoreBase baseNeoStore, IdMapper idMapper, PropertyValueLookup inputIdLookup, DataImporterMonitor monitor, PageCacheTracer pageCacheTracer)
    {
        this.baseNeoStore = baseNeoStore;
        this.idMapper = idMapper;
        this.inputIdLookup = inputIdLookup;
        this.pageCacheTracer = pageCacheTracer;
        this.cursorTracer = pageCacheTracer.createPageCursorTracer( ENTITY_IMPORTER_TAG );
        this.monitor = monitor;
        this.tokenHolders = StoreTokens.getTokenHolders(baseNeoStore, cursorTracer);
    }
    @Override
    public boolean property( String key, Object value)
    {
        assert !hasPropertyId;
        return true;
    }
    @Override
    public boolean property( String key, Object value, Object strValue)
    {
        return true;
    }

    @Override
    public boolean property( int propertyKeyId, Object value )
    {
        assert !hasPropertyId;
        entityPropertyCount++;
        return true;
    }

    @Override
    public boolean propertyId( long nextProp )
    {
        assert !hasPropertyId;
        hasPropertyId = true;
        propertyId = nextProp;
        return true;
    }

    @Override
    public boolean id( long id )
    {
        return true;
    }

    @Override
    public boolean id( Object id, Group group )
    {
        return true;
    }

    @Override
    public boolean labels( String[] labels )
    {
        return true;
    }

    @Override
    public boolean startId( long id )
    {
        return true;
    }

    @Override
    public boolean startId( Object id, Group group )
    {
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
        return true;
    }

    @Override
    public boolean type( int type )
    {
        return true;
    }

    @Override
    public boolean type( String type )
    {
        return true;
    }

    @Override
    public boolean labelField( long labelField )
    {
        return true;
    }

    @Override
    public void endOfEntity()
    {
        hasPropertyId = false;
        propertyCount += entityPropertyCount;
        entityPropertyCount = 0;

    }

    @Override
    public void close()
    {
        monitor.propertiesImported( propertyCount );
    }

    public abstract void freeUnusedIds();

    protected void putIdInIdMapper(Object id, long nodeId, Group group)
    {
        idMapper.put( id, nodeId, group );
        ((NodeInputIdPropertyLookup)inputIdLookup).save(nodeId, id, cursorTracer);
        String back = (String)inputIdLookup.lookupProperty( nodeId, PageCursorTracer.NULL);
        if (!back.equalsIgnoreCase((String)id)) {
            System.out.println("Mistmatch[" + (String) id + "][" + back + "]");
        }

    }
    protected long getNodeId( Object id, Group group )
    {
        long nodeId = idMapper.get( id, group );
        return nodeId;
    }
}

