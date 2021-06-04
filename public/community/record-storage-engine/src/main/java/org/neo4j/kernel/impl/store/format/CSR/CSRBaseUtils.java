package org.neo4j.kernel.impl.store.format.CSR;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.CSRGraph;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.loading.CSRGraphStore;
import org.neo4j.graphalgo.core.utils.export.NodeStore;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.internal.batchimport.cache.LongArray;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.Value;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class CSRBaseUtils {


    ;
    protected NeoStores neoStores = null;
    protected NodeMapping nodeMapping;
    protected CSRGraphStore csrGraphStore;
    protected NodeStore nodeStore;
    protected Graph csrHugeGraph;
    protected CSRGraph csrGraph;
    protected boolean csrSetupDone = false;
    protected HashMap<String, Integer> labelMap = new HashMap<String, Integer>(), propertyKeyMap = new HashMap<String, Integer>(), relationshipTypeMap = new HashMap<String, Integer>();
    protected HashMap<Integer, String> reversePropertyKeyMap = new HashMap<Integer, String>();
    TokenHolders tokenHolders;
    private static HashMap<String, CSRBaseUtils> INSTANCE_MAP = new HashMap<>();

    public static CSRBaseUtils getInstance( String dbName )
    {
        //String[] dbParts = dbName.split("\\.");
        //if (dbParts.length <= 2)
            return INSTANCE_MAP.get( dbName );
        //else
        //    return INSTANCE_MAP.get(dbParts[0]+"."+dbParts[2]);
    }
    public static CSRBaseUtils getInstance( NeoStores neoStores )
    {
        if (neoStores == null)
            return null;
        return getInstance( neoStores, null);
    }
    public static CSRBaseUtils getInstance( NeoStores neoStores, TokenHolders tokenHolders )
    {
        CSRBaseUtils INSTANCE = INSTANCE_MAP.get( neoStores.getDatabaseName());
        if (INSTANCE == null) {
            INSTANCE = new CSRBaseUtils(neoStores);
            INSTANCE_MAP.put(neoStores.getDatabaseName(), INSTANCE);
        }
        INSTANCE.neoStores = neoStores;
        if (tokenHolders != null) {
            INSTANCE.tokenHolders = tokenHolders;
        }
        return INSTANCE;
    }
    public CSRBaseUtils(NeoStores neoStores)
    {
        this.neoStores = neoStores;
        csrSetup();
    }

    private  String graphName = null;
    public void setCSRGraphName(String graphName)
    {
        this.graphName = graphName;
    }

    static public boolean gdsStoreExists( String dbName)
    {
        CSRBaseUtils INSTANCE = INSTANCE_MAP.get(dbName);
        if (INSTANCE == null)
            return false;
        if (INSTANCE.isCSRSetupDone())
            return true;
        return false;
    }
    public boolean isCSRSetupDone()
    {
        return csrSetupDone;
    }
    public boolean csrSetup()
    {
        if (csrSetupDone)
            return true;
        if (neoStores != null) {
            csrGraphStore = neoStores.getCSRGraphStore( graphName );
            if (csrGraphStore != null ) {
                csrHugeGraph = csrGraphStore.getGraph();;
                //csrHugeGraph = graph instanceof UnionGraph ? ((UnionGraph)graph).
                buildIncoming( csrHugeGraph );
                csrGraph = (CSRGraph)csrGraphStore.getGraph();
                nodeMapping = csrGraphStore.nodes();
                nodeStore = NodeStore.of(csrGraphStore, AllocationTracker.empty());
                getKeyMapsFromGDSStore();
                csrSetupDone = true;
                return true;
            }
        }
        return false;
    }

    public long getNodeCount(long labelId)
    {
        if (csrGraphStore == null || tokenHolders == null)
            return 0;
        long count = 0;
        for (long gdsNodeId = 0; gdsNodeId < csrHugeGraph.nodeCount(); gdsNodeId++) {
            Iterator<NodeLabel> labels = csrHugeGraph.nodeLabels( gdsNodeId ).iterator();
            while (labels.hasNext())
            {
                NodeLabel label = labels.next();
                if (labelMap.get( label.name() ) == labelId)
                    count++;
            }
        }
        return count;
    }
    public void getKeyMapsFromGDSStore()
    {
        try {
            if (csrGraphStore == null || tokenHolders == null)
                return;
            Iterator<NodeLabel> nodeLabels = csrGraphStore.nodeLabels().iterator();
            int id = 1;
            while (nodeLabels.hasNext())
            {
                NodeLabel nodeLabel = nodeLabels.next();
                System.out.println("GDS Label:"+nodeLabel.name());
                id = tokenHolders.labelTokens().getOrCreateId( nodeLabel.name());
                labelMap.put(nodeLabel.name(), id);
            }

            Iterator<RelationshipType> relTypes = csrGraphStore.relationshipTypes() .iterator();
            while (relTypes.hasNext())
            {
                RelationshipType relType = relTypes.next();
                System.out.println("GDS RelType:"+ relType.name());
                id = tokenHolders.relationshipTypeTokens().getOrCreateId( relType.name());
                relationshipTypeMap.put(relType.name(), id);
            }
            nodeLabels = csrGraphStore.nodePropertyKeys().keySet().iterator();
            while (nodeLabels.hasNext()) {
                NodeLabel nodeLabel = nodeLabels.next();
                Iterator<String> propKeys = csrGraphStore.nodePropertyKeys( nodeLabel).iterator();
                while (propKeys.hasNext())
                {
                    String propKey = propKeys.next();
                    if (!propertyKeyMap.containsKey( propKey )) {
                        System.out.println("GDS Property:"+ propKey);
                        id = tokenHolders.propertyKeyTokens().getOrCreateId(propKey);
                        propertyKeyMap.put(propKey, id);
                        reversePropertyKeyMap.put(id, propKey);
                    }
                }
            }
        } catch (IllegalStateException | KernelException e)
        {
            //do nothing - store not available.
        }
        Iterator<NamedToken> labels = tokenHolders.labelTokens().getAllTokens().iterator();
        while (labels.hasNext())
            System.out.println("Labels-:"+labels.next().name());

        Iterator<NamedToken> relTypes = tokenHolders.relationshipTypeTokens().getAllTokens().iterator();
        while (relTypes.hasNext())
            System.out.println("Rel Types-:"+relTypes.next().name());

        Iterator<NamedToken> props = tokenHolders.propertyKeyTokens().getAllTokens().iterator();
        while (props.hasNext())
            System.out.println("Property Types-:"+props.next().name());
    }

    LongArray AOForRelationshipID = null, AOIncoming = null, ALIncoming = null;
    public long getSourceId( long relId )
    {
        //AO list is sorted. So, do a binary search to see where relId fits in the offset table
        long low = 0;
        long high = AOForRelationshipID.length();//one more than size
        long middle = 0;
        while ((high - low) >= 1)
        {
            middle = low + (high - low)/2;
            long middleAlIndex = AOForRelationshipID.get ( middle );
            long nextToMiddle = (middle == AOForRelationshipID.length()-1) ? csrHugeGraph.relationshipCount() : AOForRelationshipID.get ( middle + 1 );
            if (relId >= middleAlIndex && relId < nextToMiddle)
                return middle;
            if ( relId < middleAlIndex )
                high = middle;
            else
                low = middle;
        }
        return low;
    }

    public long getTargetNodeId(long sourceNodeId, long relId)
    {
        long startRelId = AOForRelationshipID.get( sourceNodeId );
        long lastRelId = sourceNodeId == csrHugeGraph.nodeCount() -1 ? csrHugeGraph.relationshipCount()-1 :  AOForRelationshipID.get( sourceNodeId+1)-1;
        if (relId < startRelId || relId > lastRelId)
        {
            System.out.println("Error in RelId["+relId+"] for sourceNodeId["+ sourceNodeId +"]");
        }
        ProcessForTargetId processForTargetId = new ProcessForTargetId(relId, startRelId);
        try {
            csrHugeGraph.forEachRelationship(sourceNodeId, processForTargetId);
        } catch (Exception e)
        {
            System.out.println("Error-"+e.getMessage());
            csrHugeGraph.forEachRelationship(sourceNodeId, processForTargetId);
        }
        return processForTargetId.targetId;
    }

    private class ProcessForTargetId implements RelationshipConsumer
    {
        long relId, curRelId;
        public long targetId = -1;
        public int offset = 0;

        public ProcessForTargetId(long relId, long curRelId) {
            this.relId = relId;
            this.curRelId = curRelId;
        }

        @Override
        public boolean accept(long sourceNodeId, long targetNodeId) {
            if (curRelId == relId){
                targetId = targetNodeId;
                return false;
            }
            curRelId++;
            offset++;
            return true;
        }
    }

    private long getRelId(Graph graph, long sourceID, long targetID)
    {
        long[] relId = new long[1];
        relId[0] = AOForRelationshipID.get( sourceID );
        //wish there was an API for Graph that gave the offset of relationship with target as end node.
        //but without it, we have to just do a linear search - even binary search is better.
        graph.forEachRelationship( targetID, (source, target) -> {
            if ( sourceID == target )
                return false;
            relId[0]++;
            return true;
        });
        return relId[0];
    }
    private void buildIncoming( Graph hugeGraph)
    {
        NumberArrayFactory numberArrayFactory = neoStores.getNumberArrayFactory();
        //allocate memory for the required
        if (AOForRelationshipID == null)
            AOForRelationshipID = numberArrayFactory.newLongArray( hugeGraph.nodeCount(), -1, EmptyMemoryTracker.INSTANCE);
        else {
            for (long l = 0; l < AOForRelationshipID.length(); l++)
                AOForRelationshipID.set(l, -1);
        }
        if (AOIncoming == null)
            AOIncoming = numberArrayFactory.newLongArray( hugeGraph.nodeCount(), -1, EmptyMemoryTracker.INSTANCE);
        else {
            for (long l = 0; l < AOIncoming.length(); l++)
                AOIncoming.set(l, -1);
        }
        if (ALIncoming == null)
            ALIncoming = numberArrayFactory.newLongArray( hugeGraph.relationshipCount(), -1, EmptyMemoryTracker.INSTANCE);
        else {
            for (long l = 0; l < ALIncoming.length(); l++)
                ALIncoming.set(l, -1);
        }

        //first, to assign relationship Ids, build the adjecency offset table (AOForRelationshipID) for outgoing edges by just using degree of each node
        // in the same loop, initialize the count of the incoming node degree in AOIncoming
        long setValue = 0;
        for (long node = 0; node < hugeGraph.nodeCount(); node++) {
            AOForRelationshipID.set(node, setValue);
            setValue += hugeGraph.degree( node );
            hugeGraph.forEachRelationship( node, (source, target) -> {
                long newCount = AOIncoming.get( target ) + 1;
                AOIncoming.set( target, newCount );
                return true;
            });
        }
        // now, complete the building of AOIncoming
        setValue = 0;
        for (long node = 0; node < hugeGraph.nodeCount(); node++) {
            long inDegree = AOIncoming.get( node );
            AOIncoming.set( node, setValue);
            setValue += inDegree;
        }

        // now both AODegree and AOIncoming are ready
        //Now, start building ALIncoming using AOIncoming
        setValue = 0;
        ProcessForALIncoming processForALIncoming = new ProcessForALIncoming( hugeGraph );
        for (long node = 0; node < hugeGraph.nodeCount(); node++) {
            hugeGraph.forEachRelationship( node, processForALIncoming);
        }
    }

    private class ProcessForALIncoming implements RelationshipConsumer
    {
        long alIndex = -1;
        Graph graph;

        public ProcessForALIncoming(Graph graph) {
            this.graph = graph;
        }

        @Override
        public boolean accept(long sourceNodeId, long targetNodeId) {
            alIndex = AOIncoming.get( targetNodeId );
            long relId = getRelId(graph, targetNodeId, sourceNodeId);
            //make the entry is the first available slot - can be optimized
            while (ALIncoming.get(alIndex) != -1)
                alIndex++;
            ALIncoming.set(alIndex, relId);
            return true;
        }
    }

    private long getALIncomingIndex( long nodeId, long relId )
    {
        long alIndexLow = AOIncoming.get( nodeId );
        long alIndexHigh = nodeId == csrHugeGraph.nodeCount() -1 ? csrHugeGraph.nodeCount() : AOIncoming.get( nodeId + 1 );
        //the following logic HAS TO BE be changed to binary search since ALIncoming is sorted - linear search ONLY for prototype
        for (long index = alIndexLow; index < alIndexHigh; index++) {
            if (ALIncoming.get(index) == relId)
                return index;
        }
        //not found, is this an exception???
        return -1;
    }

    public long getPreviousIncomingRelID(long nodeId, long relId)
    {
        long curIndex = getALIncomingIndex(nodeId, relId);
        if (curIndex == -1)
            return AbstractBaseRecord.NO_ID;
        if (curIndex == 0 || curIndex == AOIncoming.get( nodeId ))
            return AbstractBaseRecord.NO_ID;
        return ALIncoming.get( curIndex-1);

    }
    public long getNextIncomingRelID(long nodeId, long relId)
    {
        long curIndex = getALIncomingIndex(nodeId, relId);
        if (curIndex == -1)
            return AbstractBaseRecord.NO_ID;
        if (curIndex == ALIncoming.length() || curIndex == AOIncoming.get( nodeId +1 ) -1)
            return AbstractBaseRecord.NO_ID;
        return ALIncoming.get( curIndex+1);
    }

    public double getRelProperty(long relId) {
        long nodeId = getSourceId(relId);
        long[] curRelId = new long[]{AOForRelationshipID.get(nodeId)};
        double[] propertyValue = new double[]{-1};
        csrHugeGraph.forEachRelationship(nodeId, -1, (sourceNodeId, targetNodeId, property) -> {
            if (curRelId[0] == relId) {
                propertyValue[0] = property;
                return false;
            }
            return true;
        });
        return propertyValue[0];
    }

    public Value getPropertyValue(int key, long nodeId)
    {
        return csrHugeGraph.nodeProperties(reversePropertyKeyMap.get(key)).value( nodeId);
    }

    public int getAOOffset(long sourceNodeId, long targetNodeId)
    {
        int[] count = new int[]{0};
        csrHugeGraph.forEachRelationship(sourceNodeId, (source, target) -> {
            if (target == targetNodeId)
                return false;
            count[0]++;
            return true;
        });
        return count[0];
    }
}
