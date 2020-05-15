package org.neo4j.internal.batchimport;

public class RecordSizes {
    long nodeRecordSize;
    long relationshipRecordSize;
    long propertyRecordSize;
    public RecordSizes(long nodeRecordSize, long relationshipRecordSize, long propertyRecordSize)
    {
        this.nodeRecordSize = nodeRecordSize;
        this.relationshipRecordSize = relationshipRecordSize;
        this.propertyRecordSize = propertyRecordSize;
    }

    public long getNodeRecordSize()
    {
        return this.nodeRecordSize;
    }
    public long getRelationshipRecordSize()
    {
        return this.relationshipRecordSize;
    }
    public long getPropertyRecordSize()
    {
        return this.propertyRecordSize;
    }
}
