/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

/**
 * A {@link HighLimit} record format that forces records to be split in two units more often than the original format.
 */
public class HighLimitWithSmallRecords extends HighLimit
{
    public static final String NAME = "high_limit_with_small_records";
    public static final String STORE_VERSION = "vT.H.0";
    public static final RecordFormats RECORD_FORMATS = new HighLimitWithSmallRecords();

    private static final int NODE_RECORD_SIZE = NodeRecordFormat.RECORD_SIZE / 2 + 1;
    private static final int RELATIONSHIP_RECORD_SIZE = RelationshipRecordFormat.RECORD_SIZE / 2;
    private static final int RELATIONSHIP_GROUP_RECORD_SIZE = RelationshipGroupRecordFormat.RECORD_SIZE / 2;

    private HighLimitWithSmallRecords()
    {
    }

    @Override
    public String storeVersion()
    {
        return STORE_VERSION;
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return new NodeRecordFormat( NODE_RECORD_SIZE );
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return new RelationshipRecordFormat( RELATIONSHIP_RECORD_SIZE );
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup()
    {
        return new RelationshipGroupRecordFormat( RELATIONSHIP_GROUP_RECORD_SIZE );
    }
}
