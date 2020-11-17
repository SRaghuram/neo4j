/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static com.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.NULL;

/**
 * {@link HighLimit} format which has configurable thresholds for when the various internal variants of the format kicks in.
 * Typically the "fixed references" format, which is essentially the standard community format serialized into high limit records
 * for as large records as the representation can fit. After that point the single-unit representation kicks in where data is
 * dynamically packed to fit as much as possible. If the data cannot fit in a single unit then the double-unit representation kicks in.
 * In almost all cases for reasonably large (a couple of billions) data sets the fixed reference format will be used, followed by
 * a smaller window where the single-unit representation is used. For very large data sets (tens of billions) the double-unit
 * representation kicks in.
 *
 * It's often impractical for tests to create that much data in order to fully cross all those thresholds and so
 * this format comes in handy and can allow tests exercise all the internal representations even at small data sets.
 * The format is not readable by the normal {@link HighLimit} format, but is byte-for-byte otherwise identical to it.
 */
public class HighLimitWithLowerInternalRepresentationThresholds extends HighLimit
{
    private final String name;
    private final String storeVersion;
    private final long fixedReferenceLimit;
    private final double recordUnitSizeFactor;

    public HighLimitWithLowerInternalRepresentationThresholds( String name, String storeVersion, long fixedReferenceLimit, double recordUnitSizeFactor )
    {
        this.name = name;
        this.storeVersion = storeVersion;
        this.fixedReferenceLimit = fixedReferenceLimit;
        this.recordUnitSizeFactor = recordUnitSizeFactor;
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return new NodeRecordFormat()
        {
            @Override
            protected boolean canUseFixedReferences( NodeRecord record, int recordSize )
            {
                return withinFixedReferenceLimit( record.getNextProp() ) &&
                        withinFixedReferenceLimit( record.getNextRel() );
            }

            @Override
            protected int recordUnitSize( int recordSize )
            {
                return (int) (recordSize * recordUnitSizeFactor);
            }
        };
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return new RelationshipRecordFormat()
        {
            @Override
            protected boolean canUseFixedReferences( RelationshipRecord record, int recordSize )
            {
                return withinFixedReferenceLimit( record.getFirstNode() ) &&
                        withinFixedReferenceLimit( record.getSecondNode() ) &&
                        withinFixedReferenceLimit( record.getFirstPrevRel() ) &&
                        withinFixedReferenceLimit( record.getFirstNextRel() ) &&
                        withinFixedReferenceLimit( record.getSecondPrevRel() ) &&
                        withinFixedReferenceLimit( record.getSecondNextRel() ) &&
                        withinFixedReferenceLimit( record.getNextProp() );
            }

            @Override
            protected int recordUnitSize( int recordSize )
            {
                return (int) (recordSize * recordUnitSizeFactor);
            }
        };
    }

    // TODO other formats too?

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String storeVersion()
    {
        return storeVersion;
    }

    @Override
    public int generation()
    {
        return NO_GENERATION;
    }

    private boolean withinFixedReferenceLimit( long value )
    {
        return value == NULL || value < fixedReferenceLimit;
    }
}
