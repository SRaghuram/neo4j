/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.kernel.impl.store.format.BaseRecordFormats;
import org.neo4j.kernel.impl.store.format.Capability;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.standard.LabelTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyKeyTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipTypeTokenRecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

import static org.neo4j.kernel.impl.store.format.highlimit.HighLimitFormatSettings.RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS;

/**
 * Record format with very high limits, 50-bit per ID, while at the same time keeping store size small.
 *
 * @see BaseHighLimitRecordFormat
 */
public class HighLimit extends BaseRecordFormats
{
    public static final String STORE_VERSION = StoreVersion.HIGH_LIMIT_V3_4_0.versionString();

    public static final RecordFormats RECORD_FORMATS = new HighLimit();
    public static final String NAME = "high_limit";

    protected HighLimit()
    {
        super( STORE_VERSION, StoreVersion.HIGH_LIMIT_V3_4_0.introductionVersion(), 5, Capability.DENSE_NODES,
                Capability.RELATIONSHIP_TYPE_3BYTES, Capability.SCHEMA, Capability.LUCENE_5, Capability.POINT_PROPERTIES, Capability.TEMPORAL_PROPERTIES,
                Capability.SECONDARY_RECORD_UNITS );
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return new NodeRecordFormat();
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return new RelationshipRecordFormat();
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup()
    {
        return new RelationshipGroupRecordFormat();
    }

    @Override
    public RecordFormat<PropertyRecord> property()
    {
        return new PropertyRecordFormat();
    }

    @Override
    public RecordFormat<LabelTokenRecord> labelToken()
    {
        return new LabelTokenRecordFormat();
    }

    @Override
    public RecordFormat<PropertyKeyTokenRecord> propertyKeyToken()
    {
        return new PropertyKeyTokenRecordFormat();
    }

    @Override
    public RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken()
    {
        return new RelationshipTypeTokenRecordFormat( RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS );
    }

    @Override
    public RecordFormat<DynamicRecord> dynamic()
    {
        return new DynamicRecordFormat();
    }

    @Override
    public FormatFamily getFormatFamily()
    {
        return HighLimitFormatFamily.INSTANCE;
    }

    @Override
    public String name()
    {
        return NAME;
    }
}
