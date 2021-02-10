/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.neo4j.internal.helpers.collection.Visitor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class PruningStrategyTest
{
    Segments segments = mock( Segments.class );

    List<SegmentFile> files;

    ArrayList<SegmentFile> createSegmentFiles( int size ) throws IOException
    {
        ArrayList<SegmentFile> list = new ArrayList<>( size );
        for ( int i = 0; i < size; i++ )
        {
            SegmentFile file = mock( SegmentFile.class );
            when( file.header() ).thenReturn( testSegmentHeader( i ) );
            when( file.size() ).thenReturn( 1L );
            list.add( file );
        }
        return list;
    }

    @BeforeEach
    public void mockSegmentVisitor()
    {
        doAnswer( invocation ->
        {
            Visitor<SegmentFile,RuntimeException> visitor = invocation.getArgument( 0 );
            ListIterator<SegmentFile> itr = files.listIterator( files.size() );
            boolean terminate = false;
            while ( itr.hasPrevious() && !terminate )
            {
                terminate = visitor.visit( itr.previous() );
            }
            return null;
        } ).when( segments ).visitBackwards( any() );
    }

    private SegmentHeader testSegmentHeader( long value )
    {
        return new SegmentHeader( -1, -1, value - 1, -1 );
    }
}
