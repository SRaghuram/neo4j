package org.neo4j.utils.runutils;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class Distributor<R extends AbstractBaseRecord>
{
    
        long maxCount;
        protected long recordsPerCPU;
        protected int numThreads;
        public Distributor(long maxCount, int numThreads)
        {
            this.maxCount = maxCount;
            this.numThreads = numThreads;
            this.recordsPerCPU = (maxCount/numThreads) + 1;
        }
        public int[] whichQ(R record, int[] currentQ) {
            if (currentQ == null)
                currentQ = new int[]{0};
            currentQ[0]++; 
            currentQ[0] %= numThreads;
            return currentQ;
        }
        public long getRecordsPerCPU()
        {
            return recordsPerCPU;
        }
        public int getNumThreads()
        {
            return numThreads;
        }
    public class NodeDistributor extends Distributor<NodeRecord>
    {
        public NodeDistributor( long maxCount, int numThreads )
        {
            super( maxCount, numThreads );
        }

        /*@Override
        public int[] whichQ( NodeRecord node, int[] currentQ )
        {
            return new int[] {((int) (node.getId() / this.recordsPerCPU))};
        }*/
    }

    public class RelationshipDistributor extends Distributor<RelationshipRecord>
    {
        public RelationshipDistributor( long maxCount, int numThreads )
        {
            super( maxCount, numThreads );
        }

        @Override
        public int[] whichQ( RelationshipRecord relationship, int[] currentQ )
        {
            int qIndex1 = (int) (relationship.getFirstNode() / this.recordsPerCPU);
            int qIndex2 = (int) (relationship.getSecondNode() / this.recordsPerCPU);
            if ( qIndex1 == qIndex2 )
            {
                return new int[] {qIndex1};
            }
            else
            {
                return new int[] {qIndex1, qIndex2};
            }
        }
    }

    public class PropertyDistributor extends Distributor<PropertyRecord>
    {
        public PropertyDistributor( long maxCount, int numThreads )
        {
            super( maxCount, numThreads );
        }

    }
}
