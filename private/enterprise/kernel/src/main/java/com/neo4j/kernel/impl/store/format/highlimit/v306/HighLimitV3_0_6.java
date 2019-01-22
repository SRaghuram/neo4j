/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit.v306;

import com.neo4j.kernel.impl.store.format.highlimit.DynamicRecordFormat;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimitFormatFamily;

import org.neo4j.kernel.impl.store.format.BaseRecordFormats;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.LuceneCapability;
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

/**
 * Record format with very high limits, 50-bit per ID, while at the same time keeping store size small.
 * With fixed references ability.
 *
 * @see BaseHighLimitRecordFormatV3_0_6
 */
public class HighLimitV3_0_6 extends BaseRecordFormats
{
    /**
     * Default maximum number of bits that can be used to represent id
     */
    static final int DEFAULT_MAXIMUM_BITS_PER_ID = 50;

    public static final String STORE_VERSION = StoreVersion.HIGH_LIMIT_V3_0_6.versionString();
    public static final RecordFormats RECORD_FORMATS = new HighLimitV3_0_6();
    public static final String NAME = "high_limitV3_0_6";

    public HighLimitV3_0_6()
    {
        super( STORE_VERSION, StoreVersion.HIGH_LIMIT_V3_0_6.introductionVersion(), 2, LuceneCapability.DENSE_NODES,
                LuceneCapability.SCHEMA, LuceneCapability.LUCENE_5, LuceneCapability.SECONDARY_RECORD_UNITS );
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return new NodeRecordFormatV3_0_6();
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return new RelationshipRecordFormatV3_0_6();
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup()
    {
        return new RelationshipGroupRecordFormatV3_0_6();
    }

    @Override
    public RecordFormat<PropertyRecord> property()
    {
        return new PropertyRecordFormatV3_0_6();
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
        return new RelationshipTypeTokenRecordFormat();
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
