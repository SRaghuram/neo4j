/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
class RelationshipReferenceEncodingTest
{
    /**
     * The current scheme only allows us to use 58 bits for a reference. Adhere to that limit here.
     */
    private static final long MASK = numberOfBits( Reference.MAX_BITS );
    private static final int PAGE_SIZE = 100;

    @Inject
    private RandomRule random;
    private final StubPageCursor cursor = new StubPageCursor( 0, PAGE_SIZE );

    @Test
    void shouldEncodeRandomLongs()
    {
        for ( int i = 0; i < 100_000_000; i++ )
        {
            long reference = limit( random.nextLong() );
            assertDecodedMatchesEncoded( reference );
        }
    }

    @Test
    void relativeReferenceConversion()
    {
        long basis = 0xBABE;
        long absoluteReference = 0xCAFEBABE;

        long relative = Reference.toRelative( absoluteReference, basis );
        assertEquals( 0xCAFE0000, relative, "Should be equal to difference of reference and base reference" );

        long absoluteCandidate = Reference.toAbsolute( relative, basis );
        assertEquals( absoluteReference, absoluteCandidate, "Converted reference should be equal to initial value" );
    }

    private static long numberOfBits( int count )
    {
        long result = 0;
        for ( int i = 0; i < count; i++ )
        {
            result = (result << 1) | 1;
        }
        return result;
    }

    private static long limit( long reference )
    {
        boolean positive = true;
        if ( reference < 0 )
        {
            positive = false;
            reference = ~reference;
        }

        reference &= MASK;

        if ( !positive )
        {
            reference = ~reference;
        }
        return reference;
    }

    private void assertDecodedMatchesEncoded( long reference )
    {
        cursor.setOffset( 0 );
        Reference.encode( reference, cursor );

        cursor.setOffset( 0 );
        long read = Reference.decode( cursor );
        assertEquals( reference, read );
    }
}
