package org.neo4j.propertystorereorg;

import static java.lang.System.arraycopy;

import java.util.Arrays;
import java.util.Set;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.store.StoreAccess;
import org.neo4j.utils.NodeLabelReader;
import org.neo4j.utils.ToolUtils;
import org.neo4j.utils.runutils.CommandProcessor;
import org.neo4j.utils.runutils.RecordProcessor;
import org.neo4j.utils.runutils.StoppableRunnable;
public class NodeLabelCountProcess implements RecordProcessor<Abstract64BitRecord> {

    NodeStore store;
    StoreAccess storeAccess;
    public long[] labelCount = null, labelCountSave = null;
    private long[] labelOrderIndex = null;
    private long inUseNodes = 0;
    public enum STATUS
    {
        UnInitialized,
        LabelCounts,
        NodeSortByLabel,
        NodeArrayMap
    }
    public STATUS status = STATUS.UnInitialized;
    public NodeLabelCountProcess( NodeStore store, StoreAccess storeAccess ) {
        this.store = store;
        this.storeAccess = storeAccess;
        labelCount = new long[(int)storeAccess.getLabelNameStore().getHighestPossibleIdInUse()+1];
        labelCountSave = new long[(int)storeAccess.getLabelNameStore().getHighestPossibleIdInUse()+1];
        labelOrderIndex = new long[(int)storeAccess.getLabelNameStore().getHighestPossibleIdInUse()+1];
    }


    @Override
    public void process( Abstract64BitRecord record )
    {
        int i = 2;
        try
        {
        if (record instanceof NodeRecord)
        {
            NodeRecord node = (NodeRecord)record;
            Set<Long> nodeLabels = NodeLabelReader.getListOfLabels(node, storeAccess );
            Long[] labels = nodeLabels.toArray( new Long[0] );
            if (status == STATUS.LabelCounts)
            {
                if (nodeLabels.size() == 0)
                    labelCount[0]++;
                else
                    for (long label : nodeLabels)
                        synchronized(labelCount)
                        {
                            labelCount[(int)label]++;
                        }
            } else if (status == STATUS.NodeSortByLabel)
            {
                int lblIndex = 0;
                if (labels.length > 0)
                    lblIndex = translate(labels);
                synchronized(labelCount)
                {
                    labelCount[lblIndex]++;
                    inUseNodes++;
                }
                
            } else if (status == STATUS.NodeArrayMap)
            {                  
                long nodeMap = -1;
                int labelIndex = 0; 
                    
                if (labels.length > 0)
                    labelIndex = translate(labels);
                
                synchronized(labelCount)
                {
                    nodeMap = labelCount[labelIndex]++;
                }
                NodeIdMapByLabel.nodeIdMap.set( nodeMap , record.getId());
            }
        }
        } catch (Exception e) {
            System.out.println("NodeLabelCountProcess"+e.getMessage());
        }
    }
    
    private int translate(Long[] labels)
    {
        long[] lbls = new long[labels.length];
        for (int i = 0; i < labels.length; i++)
            lbls[i] = ((long)labels[i].intValue()  << 32) | labelOrderIndex[labels[i].intValue()];
        ToolUtils.sortIndexValueArray(lbls, 0, labels.length);
        return ToolUtils.getValue( lbls[0] );
    }

    @Override
    public void close()
    {
        for (int i = 0; i < labelCount.length; i++)
        {
            System.out.println("["+i+"] ["+ 
                    (i == 0 ? "No Labels" : storeAccess.getRawNeoStore().getLabelTokenStore().getToken( i ).name())
                        + "] [" +ToolUtils.getIndex(labelCount[i]) + "] ["+ ToolUtils.getValue(labelCount[i])  + "]");
        }
        if (status == STATUS.LabelCounts)
        {
            ToolUtils.sortIndexValueArray(labelCount, 1, labelCount.length);
            for (int i = 0; i < labelCount.length; i++)
            {          
                System.out.println("["+i+"] ["+ ToolUtils.getIndex(labelCount[i]) + "] ["+ ToolUtils.getValue(labelCount[i])  +"]");
            }
            ToolUtils.saveMessage(ToolUtils.getMaxIds(storeAccess.getRawNeoStore()));
            ToolUtils.saveMessage( storeAccess.getAccessStatsStr() );
            ToolUtils.printSavedMessage(false);
   
            for (int i = 0; i < labelCount.length; i++)
            {
                labelOrderIndex[ToolUtils.getIndex( labelCount[i] )]  = i ;
                labelCount[ i ] = 0;
            }
        } else if (status == STATUS.NodeSortByLabel)
        {
            int sum = 0;
            for (int i = 0; i < labelCount.length; i++)
            {
                long val =  ToolUtils.getValue( labelCount[i] );
                labelCountSave[i] = labelCount[i];
                labelCount[i] = sum;
                sum += val;    
            }
            if (NodeIdMapByLabel.nodeIdMap == null)
                new NodeIdMapByLabel();
        }  else if (status == STATUS.NodeArrayMap)
        {
            //just a sanity check to make sure all nodes have the mapping
            for (long id = 0; id < inUseNodes; id++)
                if (NodeIdMapByLabel.nodeIdMap.get( id ) == -1)
                    System.out.println("ID [" + id + "] not mapped");
        }
        System.out.println("--------");
        for (int i = 0; i < labelCount.length; i++)
        {          
            System.out.println("["+i+"] ["+ ToolUtils.getIndex(labelCount[i]) + "] ["+ ToolUtils.getValue(labelCount[i]) 
                    +"] -- ["+ ToolUtils.getIndex(labelCountSave[i]) + "] ["+ ToolUtils.getValue(labelCountSave[i]) +"]");
        }
    }
    
    public class StateChanger implements CommandProcessor
    {

        @Override
        public boolean runCommand(StoppableRunnable runProcess)
        {
            if (status == STATUS.UnInitialized)
                status = STATUS.LabelCounts;
            else if (status == STATUS.LabelCounts)
                status = STATUS.NodeSortByLabel; 
            else if (status == STATUS.NodeSortByLabel)
            {
                status = STATUS.NodeArrayMap;
                runProcess.setParallel( false );
            }
            return true;
        }
        
    }
    
    static long[] sort( long[] existing)
    {
        long[] result = new long[existing.length];
        arraycopy( existing, 0, result, 0, existing.length );
        Arrays.sort( result );
        return result;
    }
}
