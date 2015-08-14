package org.neo4j.store;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.*;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class StoreAccess extends org.neo4j.kernel.impl.store.StoreAccess
{
    private interface IStores
    {
        public int getProximity(StoreAccess storeAccess);
    }

    public enum STORES implements IStores
    {
        NodeStore
        {
            @Override
            public int getProximity(StoreAccess storeAccess)
            {
                return ((NodeStore)storeAccess.getNodeStore()).getPageSize()/storeAccess.getNodeStore().getRecordSize();
            }
           
        },
        RelationshipStore
        {
            @Override
            public int getProximity(StoreAccess storeAccess)
            {
                return ((RelationshipStore)storeAccess.getRelationshipStore()).getPageSize()/storeAccess.getRelationshipStore().getRecordSize();
            }
           
        },
        PropertyStore
        {
            @Override
            public int getProximity(StoreAccess storeAccess)
            {
                return ((PropertyStore)storeAccess.getPropertyStore()).getPageSize()/storeAccess.getPropertyStore().getRecordSize();
            }         
        },
        StringStore
        {
            @Override
            public int getProximity(StoreAccess storeAccess)
            {
                return ((DynamicStringStore)storeAccess.getStringStore()).getPageSize()/storeAccess.getStringStore().getRecordSize();
            }
        },
        ArrayStore
        {
            @Override
            public int getProximity(StoreAccess storeAccess)
            {
                return ((DynamicArrayStore)storeAccess.getArrayStore()).getPageSize()/storeAccess.getArrayStore().  getRecordSize();
            }
        };
    }

    private AccessStats[] accessStats = null;

    public StoreAccess( NeoStore neoStore )
    {
        super( neoStore );
        accessStats = new AccessStats[STORES.values().length];
        for (STORES store : STORES.values() )
            accessStats[store.ordinal()] = new AccessStats( store, store.getProximity( this) );
    }

    public PropertyRecord getPropertyRecord( long id )
    {
        return getPropertyRecord( id, false );
    }
    public PropertyRecord getPropertyRecord( long id, boolean force )
    {
        accessStats[STORES.PropertyStore.ordinal()].upRead( id );
        if (force)
            return this.getPropertyStore().forceGetRecord( id );
        else
            return this.getPropertyStore().getRecord( id ); 
    }

    public NodeRecord getNodeRecord( long id )
    {
        return getNodeRecord( id, false );
    }
    public NodeRecord getNodeRecord( long id, boolean force )
    {
        accessStats[STORES.NodeStore.ordinal()].upRead( id );
        if (force)
            return this.getNodeStore().forceGetRecord( id );
        else
            return this.getNodeStore().getRecord( id );
    }
    public RelationshipRecord getRelationshipRecord( long id )
    {
        return getRelationshipRecord( id, false );
    }

    public RelationshipRecord getRelationshipRecord( long id, boolean force )
    {
        accessStats[STORES.RelationshipStore.ordinal()].upRead( id );
        if (force)
            return this.getRelationshipStore().forceGetRecord( id );
        else
            return this.getRelationshipStore().getRecord( id );
    }

    public DynamicRecord getStringRecord( long id, boolean force )
    {
        accessStats[STORES.StringStore.ordinal()].upRead( id );
        if (force)
            return this.getStringStore().forceGetRecord( id );
        else
            return this.getStringStore().getRecord( id );
    }
    public DynamicRecord getArrayRecord( long id, boolean force )
    {
        accessStats[STORES.ArrayStore.ordinal()].upRead( id );
        if (force)
            return this.getArrayStore().forceGetRecord( id );
        else
            return this.getArrayStore().getRecord( id );
    }
    public class AccessStats
    {
        private long reads = 0, writes = 0, inUse = 0;
        private long randomReads = 0, randomWrites = 0, randomReadsEst = 0;
        int proximityValue = 0;
        STORES storeType;

        public AccessStats( STORES type, int proximity )
        {
            this.storeType = type;
            this.proximityValue = proximity;
        }

        public String toString()
        {
            if ( reads == 0 && writes == 0 && randomReads == 0 && randomReadsEst == 0 )
                return "";
            StringBuffer buf = new StringBuffer();
            buf.append( this.storeType.name()+" [" );
            if ( inUse > 0 )
                buf.append( "InUse:" + Long.toString( inUse ) + ", " );
            boolean start = true;
            if ( reads > 0 )
            {
                buf.append( "Reads:" + Long.toString( reads ));
                start = false;
            }
            if (randomReads > 0)
            {               
                buf.append((start ? "Random" : "-Random:") + Long.toString( randomReads ) );
                start = false;
            }
            //if (randomReads > 0)
            //  buf.append("-Random:"+Long.toString(randomReads));
            if ( randomReadsEst > 0 )
                buf.append( (start ? "Scattered:" : "-Scattered:") + Long.toString( randomReadsEst ) );
            //if (reads > 0 || randomReads > 0 || randomReadsEst > 0)
            //  buf.append(Long.toString(reads) + ":" + Long.toString(randomReads)+":"+ Long.toString(randomReadsEst));
            if ( writes > 0 )
                buf.append( ", " + Long.toString( writes ) + ":" + Long.toString( randomWrites ) );
            buf.append( "] " );
           
            int scatterIndex = 0;
            if ( randomReadsEst > 0)
            {
                reads = reads == 0 ? randomReadsEst : reads;
                scatterIndex  = (int)((randomReadsEst*100)/reads);
            }
               
            buf.append( "[ScatterIndex: "+ scatterIndex + " %]" );
            if (scatterIndex > 0.5)
                buf.append("\n *** [Property Store reorgization is recommended for optimal performance] ***");
            return buf.toString();
        }

        public void reset( int proximityValue )
        {
            reads = 0;
            writes = 0;
            randomReads = 0;
            randomReadsEst = 0;
            randomWrites = 0;
            inUse = 0;
            this.proximityValue = proximityValue;
        }

        private long prevReadId, prevWriteId;

        protected void upRead( long id )
        {
            if ( prevReadId != id )
            {
                reads++;
                incrementRandomReads( id, prevReadId );
                prevReadId = id;
            }
        }

        private boolean closeBy( long id1, long id2 )
        {
            if ( id1 < 0 || id2 < 0 )
                return true;
            if ( Math.abs( id2 - id1 ) < this.proximityValue )
                return true;
            return false;
        }

        protected void upWrite( long id )
        {
            if ( prevWriteId != id )
            {
                writes++;
                if ( id > 0 && !closeBy( id, prevWriteId ) )
                    randomWrites++;
                prevWriteId = id;
            }
        }

        public synchronized void incrementRandomReads( long id1, long id2 )
        {
            if ( !closeBy( id1, id2 ) )
                randomReads++;
        }

        public synchronized void incrementRREstimate( long id1, long id2 )
        {
            if ( !closeBy( id1, id2 ) )
                randomReadsEst++;
        }

        public synchronized void upInUse()
        {
            inUse++;
        }
    }

    public String getAccessStatsStr()
    {
        String msg = "";
        for ( AccessStats accessStat : accessStats )
        {
            if (accessStat.toString().length() != 0) 
                msg += accessStat.toString() +  "\n"+"\t";         
        }
        return msg;
    }

    public void resetStats( int proximityValue )
    {
        for ( AccessStats accessStat : accessStats )
            accessStat.reset( proximityValue );
    }

    public AccessStats getAccessStats( CommonAbstractStore store )
    {
        if (store instanceof NodeStore )
        {
            return accessStats[STORES.NodeStore.ordinal()];
        } else if (store instanceof RelationshipStore)
        {
            return accessStats[STORES.RelationshipStore.ordinal()];
        } else if (store instanceof PropertyStore)
        {
            return accessStats[STORES.PropertyStore.ordinal()]; 
        }else if (store instanceof DynamicStringStore)
        {
            return accessStats[STORES.StringStore.ordinal()];
        }else if (store instanceof DynamicArrayStore)
        {
            return accessStats[STORES.ArrayStore.ordinal()];
        }
        return null;
    }
    
    public <RECORD extends AbstractBaseRecord> RECORD getRecord(RecordStore<RECORD> store, long id)
    {       
        RECORD record =  null;
        if (store instanceof NodeStore )
        {
            record =  (RECORD)this.getNodeRecord( id, true );
            return record;
        } else if (store instanceof RelationshipStore)
        {
            record =  (RECORD)this.getRelationshipRecord( id, true );
            return record;
        } else if (store instanceof PropertyStore)
        {
            record =  (RECORD)this.getPropertyRecord( id, true );
            return record;
        }else if (store instanceof DynamicStringStore &&  ((CommonAbstractStore)store).getIdType().name().equals( IdType.STRING_BLOCK.name() ))
        {
            record =  (RECORD)this.getStringRecord( id, true );
            return record;
        }else if (store instanceof DynamicArrayStore)
        {
            record =  (RECORD)this.getArrayRecord( id, true );
            return record;
        }
        record =  store.forceGetRecord( id );
        return record;
    }
    
    static public <RECORD extends AbstractBaseRecord> RECORD getRecord(RecordStore<RECORD> store, long id, StoreAccess access)
    {       
        RECORD record =  null;
        if (store instanceof NodeStore )
        {
            record =  (RECORD)access.getNodeRecord( id, true );
            return record;
        } else if (store instanceof RelationshipStore)
        {
            record =  (RECORD)access.getRelationshipRecord( id, true );
            return record;
        } else if (store instanceof PropertyStore)
        {
            record =  (RECORD)access.getPropertyRecord( id, true );
            return record;
        }else if (store instanceof DynamicStringStore &&  ((CommonAbstractStore)store).getIdType().name().equals( IdType.STRING_BLOCK.name() ))
        {
            record =  (RECORD)access.getStringRecord( id, true );
            return record;
        }else if (store instanceof DynamicArrayStore)
        {
            record =  (RECORD)access.getArrayRecord( id, true );
            return record;
        }
        record =  store.forceGetRecord( id );
        return record;
    }
}
