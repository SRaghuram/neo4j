/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.internal.id.BatchingIdSequence;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.format.AbstractRecordFormatTest;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordGenerators.Generator;
import org.neo4j.kernel.impl.store.format.RecordKey;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static java.lang.System.currentTimeMillis;

class HighLimitRecordFormatTest extends AbstractRecordFormatTest
{
    HighLimitRecordFormatTest()
    {
        super( HighLimit.RECORD_FORMATS, 50, 50 );
    }

    @Test
    void nodeGrowAndShrink() throws Exception
    {
        verifyWriteAndReadWhenGrowingAndShrinking( formats::node, generators::node, keys::node );
    }

    @Test
    void relationshipGrowAndShrink() throws Exception
    {
        verifyWriteAndReadWhenGrowingAndShrinking( formats::relationship, generators::relationship, keys::relationship );
    }

    @Test
    void relationshipGroupGrowAndShrink() throws Exception
    {
        verifyWriteAndReadWhenGrowingAndShrinking( formats::relationshipGroup, generators::relationshipGroup, keys::relationshipGroup );
    }

    private <R extends AbstractBaseRecord> void verifyWriteAndReadWhenGrowingAndShrinking(
            Supplier<RecordFormat<R>> formatSupplier,
            Supplier<Generator<R>> generatorSupplier,
            Supplier<RecordKey<R>> keySupplier ) throws IOException
    {
        // GIVEN
        RecordFormat<R> format = formatSupplier.get();
        RecordKey<R> key = keySupplier.get();
        Generator<R> generator = generatorSupplier.get();
        int recordSize = format.getRecordSize( new IntStoreHeader( DATA_SIZE ) );
        BatchingIdSequence idSequence = new BatchingIdSequence( 1 );
        // WHEN
        PageCursor cursor = ByteArrayPageCursor.wrap( recordSize );
        long time = currentTimeMillis();
        long endTime = time + TEST_TIME;
        long i = 0;
        for ( ; i < TEST_ITERATIONS && currentTimeMillis() < endTime; i++ )
        {
            // First generate a guaranteed double-unit record
            R doubleUnitRecord = continueUntil(
                    () -> prepared( generator.get( recordSize, format, random.nextLong( 1, format.getMaxId() ) ), format, recordSize, idSequence ),
                    AbstractBaseRecord::requiresSecondaryUnit );
            R readDoubleUnitRecord = verifyWriteAndReadRecord( true, format, key, recordSize, idSequence, cursor, i, doubleUnitRecord );

            // Then change it to become single-unit, do this by using the generator to generate a new record and then copy
            // the state of the secondary unit and ID from the previous double-unit into it. This mimics loading the record
            // and changing its data and doesn't require special generator methods for randomly changing a record.
            R shrunkRecord = continueUntil(
                    () -> prepared( generator.get( recordSize, format, random.nextLong( 1, format.getMaxId() ) ), format, recordSize, idSequence ),
                    AbstractBaseRecord::isUseFixedReferences );
            shrunkRecord.setId( readDoubleUnitRecord.getId() );
            shrunkRecord.setRequiresSecondaryUnit( readDoubleUnitRecord.requiresSecondaryUnit() );
            shrunkRecord.setSecondaryUnitIdOnLoad( readDoubleUnitRecord.getSecondaryUnitId() );
            shrunkRecord.setUseFixedReferences( readDoubleUnitRecord.isUseFixedReferences() );
            verifyWriteAndReadRecord( true, format, key, recordSize, idSequence, cursor, i, shrunkRecord );
        }
    }

    private <R extends AbstractBaseRecord> R prepared( R record, RecordFormat<R> format, int recordSize, BatchingIdSequence idSequence )
    {
        format.prepare( record, recordSize, idSequence, PageCursorTracer.NULL );
        return record;
    }

    private static <T> T continueUntil( Supplier<T> generator, Predicate<T> filter )
    {
        T result;
        do
        {
            result = generator.get();
        }
        while ( !filter.test( result ) );
        return result;
    }
}
