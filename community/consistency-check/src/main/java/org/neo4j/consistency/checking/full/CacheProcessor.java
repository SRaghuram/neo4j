package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.RecordType;
import org.neo4j.consistency.checking.full.StoreProcessorTask.Scanner;
import org.neo4j.consistency.report.ConsistencyReport.DynamicConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicLabelConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.LabelTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyKeyTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipTypeConsistencyReport;
import org.neo4j.consistency.store.StoreAccess;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

public class CacheProcessor extends StoreProcessor
{
    CacheAction action;
    static StoreAccess storeAccess;
    static StoreProcessor storeP = null;

    //static int[] cacheFields = null;
    public CacheProcessor( FullCheckNewUtils.Stages stage, CacheAction action, StoreAccess storeAccess,
            int... cacheFields )
    {
        super( null, null, stage );
        this.setCacheFields( cacheFields );
        this.action = action;
        this.storeAccess = storeAccess;
        //this.cacheFields = cacheFields;
    }

    public CacheProcessor( FullCheckNewUtils.Stages stage, CacheAction action, StoreAccess storeAccess,
            StoreProcessor storeP )
    {
        super( null, null, stage );
        this.action = action;
        this.storeAccess = storeAccess;
        this.storeP = storeP;
    }

    public void cacheLabels( RecordStore store )
    {
    }

    private interface CacheProcessing
    {
        public boolean processCache();

        public void setCache( int[] fields );
    }

    public static enum CacheAction implements CacheProcessing
    {
        GET_NEXTREL
        {
            @Override
            public boolean processCache()
            {
                FullCheckNewUtils.NewCCCache.cleanCache();
                RecordStore<NodeRecord> nodeStore = storeAccess.getNodeStore();
                long[] fields = new long[] {1, 0, -1};
                for ( NodeRecord node : Scanner.scan( nodeStore, storeAccess, true ) )
                {
                    if ( node.inUse() )
                    {
                        fields[2] = node.getNextRel();
                        FullCheckNewUtils.NewCCCache.putToCache( node.getId(), fields );
                    }
                }
                return true;
            }

            @Override
            public void setCache( int[] cacheFields )
            {
                if ( cacheFields != null && cacheFields.length > 0 )
                    FullCheckNewUtils.NewCCCache.resetFieldOffsets( cacheFields );
            }
        },
        CLEAR_CACHE
        {
            @Override
            public boolean processCache()
            {
                // TODO Auto-generated method stub
                FullCheckNewUtils.NewCCCache.initCache();
                return true;
            }
        },
        CHECK_NEXTREL
        {
            @Override
            public boolean processCache()
            {
                RecordStore<NodeRecord> nodeStore = storeAccess.getNodeStore();
                for ( long nodeId = 0; nodeId < nodeStore.getHighId(); nodeId++ )
                {
                    int nextRelFromCache = (int) FullCheckNewUtils.NewCCCache.getFromCache( nodeId )[1];
                    if ( nextRelFromCache == 0 )
                    {
                        NodeRecord node = nodeStore.forceGetRaw( nodeId );
                        if ( storeP != null && !node.isDense() )
                            storeP.processNode( nodeStore, node );
                    }
                }
                return true;
            }
        };
        @Override
        public void setCache( int[] cacheFields )
        {
            // do nothing		
        }
    }

    @Override
    protected void checkNode( RecordStore<NodeRecord> store, NodeRecord node,
            RecordCheck<NodeRecord,NodeConsistencyReport> checker )
    {
        // TODO Auto-generated method stub
    }

    @Override
    protected void checkRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel,
            RecordCheck<RelationshipRecord,RelationshipConsistencyReport> checker )
    {
        // TODO Auto-generated method stub
    }

    @Override
    protected void checkProperty( RecordStore<PropertyRecord> store, PropertyRecord property,
            RecordCheck<PropertyRecord,PropertyConsistencyReport> checker )
    {
        // TODO Auto-generated method stub
    }

    @Override
    protected void checkRelationshipTypeToken( RecordStore<RelationshipTypeTokenRecord> store,
            RelationshipTypeTokenRecord record,
            RecordCheck<RelationshipTypeTokenRecord,RelationshipTypeConsistencyReport> checker )
    {
        // TODO Auto-generated method stub
    }

    @Override
    protected void checkLabelToken( RecordStore<LabelTokenRecord> store, LabelTokenRecord record,
            RecordCheck<LabelTokenRecord,LabelTokenConsistencyReport> checker )
    {
        // TODO Auto-generated method stub
    }

    @Override
    protected void checkPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord record,
            RecordCheck<PropertyKeyTokenRecord,PropertyKeyTokenConsistencyReport> checker )
    {
        // TODO Auto-generated method stub
    }

    @Override
    protected void checkDynamic( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
            RecordCheck<DynamicRecord,DynamicConsistencyReport> checker )
    {
        // TODO Auto-generated method stub
    }

    @Override
    protected void checkDynamicLabel( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
            RecordCheck<DynamicRecord,DynamicLabelConsistencyReport> checker )
    {
        // TODO Auto-generated method stub
    }

    @Override
    protected void checkRelationshipGroup( RecordStore<RelationshipGroupRecord> store, RelationshipGroupRecord record,
            RecordCheck<RelationshipGroupRecord,RelationshipGroupConsistencyReport> checker )
    {
        // TODO Auto-generated method stub
    }
}
