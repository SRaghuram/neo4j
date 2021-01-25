/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.internal.id.BatchingIdSequence;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;

import static com.neo4j.kernel.impl.store.format.highlimit.HighLimitWithLowerInternalRepresentationThresholdsSmallFactory.FIXED_REFERENCE_THRESHOLD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

class HighLimitLTMetaTest
{
    private static final int REP_FIXED = 0;
    private static final int REP_SINGLE = 1;
    private static final int REP_DOUBLE = 2;

    @Test
    void shouldPickFixedReferenceRepresentationForSmallFieldsForRelationship()
    {
        shouldPickCorrectRepresentation( REP_FIXED, RecordFormats::relationship,
                record -> record.initialize( true, 12, 192, 10, 2, 1, NULL_REFERENCE.longValue(), 3, 453, true, true ) );
    }

    @Test
    void shouldPickSingleRepresentationForTooLargeFieldForFixedForRelationship()
    {
        shouldPickCorrectRepresentation( REP_SINGLE, RecordFormats::relationship,
                record -> record.initialize( true, NULL_REFERENCE.longValue(), FIXED_REFERENCE_THRESHOLD + 1, 10, 2, 1, NULL_REFERENCE.longValue(), 3,
                        NULL_REFERENCE.longValue(), true, true ) );
    }

    @Test
    void shouldPickDoubleRepresentationForTooMuchDataForSingleForRelationship()
    {
        shouldPickCorrectRepresentation( REP_DOUBLE, RecordFormats::relationship,
                record -> record.initialize( true, 12345, FIXED_REFERENCE_THRESHOLD + 1, 10, 2, 1, 45893, 3, 489323, true, true ) );
    }

    @Test
    void shouldPickFixedReferenceRepresentationForSmallFieldsForNode()
    {
        shouldPickCorrectRepresentation( REP_FIXED, RecordFormats::node,
                record -> record.initialize( true, 1, false, 2, Record.NO_LABELS_FIELD.longValue() ) );
    }

    @Test
    void shouldPickSingleReferenceRepresentationForTooLargeFieldForFixedForNode()
    {
        shouldPickCorrectRepresentation( REP_SINGLE, RecordFormats::node,
                record -> record.initialize( true, FIXED_REFERENCE_THRESHOLD + 10, false, 2, Record.NO_LABELS_FIELD.longValue() ) );
    }

    @Test
    void shouldPickDoubleReferenceRepresentationForTooMuchDataForSingleForNode()
    {
        shouldPickCorrectRepresentation( REP_DOUBLE, RecordFormats::node,
                record -> record.initialize( true, 1 << 23, true, 1 << 30, 1 << 30 ) );
    }

    private <R extends AbstractBaseRecord> void shouldPickCorrectRepresentation( int expectedRepresentation,
            Function<RecordFormats,RecordFormat<R>> formatFunction, Consumer<R> initializer )
    {
        // given
        RecordFormats formats = new HighLimitWithLowerInternalRepresentationThresholdsSmallFactory().newInstance();
        RecordFormat<R> format = formatFunction.apply( formats );
        int recordSize = format.getRecordSize( NO_STORE_HEADER );
        R record = format.newRecord();
        initializer.accept( record );

        // when
        format.prepare( record, recordSize, new BatchingIdSequence( 1 ), PageCursorTracer.NULL );

        // then
        switch ( expectedRepresentation )
        {
        case REP_FIXED:
            assertThat( record.isUseFixedReferences() ).isTrue();
            assertThat( record.requiresSecondaryUnit() ).isFalse();
            break;
        case REP_SINGLE:
            assertThat( record.isUseFixedReferences() ).isFalse();
            assertThat( record.requiresSecondaryUnit() ).isFalse();
            break;
        case REP_DOUBLE:
            assertThat( record.isUseFixedReferences() ).isFalse();
            assertThat( record.requiresSecondaryUnit() ).isTrue();
            break;
        default:
            throw new IllegalArgumentException();
        }
    }
}
