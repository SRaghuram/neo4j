package org.neo4j.unsafe.impl.batchimport;

import static org.neo4j.unsafe.impl.batchimport.RecordIdIterator.forwards;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.staging.BatchFeedStep;
import org.neo4j.unsafe.impl.batchimport.staging.ReadRecordsStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;

public class NodeDegreeCountStage extends Stage
{
    public NodeDegreeCountStage( String topic, Configuration config, RelationshipStore store,
            NodeRelationshipCache cache )
    {
        super( "Node Degree counts: " + topic, config, ORDER_SEND_DOWNSTREAM );
        add( new BatchFeedStep( control(), config, forwards( 0, store.getHighId(), config ), store.getRecordSize()) );
        add( new ReadRecordsStep<>( control(), config, true, store, null ) );
        add( new CalculateDenseNodesStep( control(), config, cache ) );
    }
}
