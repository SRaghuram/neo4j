/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReaderTest
{
    private final FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
    private final StoreChannel channel = mock( StoreChannel.class );
    private final File file = mock( File.class );

    @Test
    public void shouldCloseChannelOnClose() throws Exception
    {
        // given
        when( fsa.open( file, OpenMode.READ ) ).thenReturn( channel );
        Reader reader = new Reader( fsa, file, 0 );

        // when
        reader.close();

        // then
        verify( channel ).close();
    }

    @Test
    public void shouldUpdateTimeStamp() throws Exception
    {
        // given
        Reader reader = new Reader( fsa, file, 0 );

        // when
        int expected = 123;
        reader.setTimeStamp( expected );

        // then
        assertEquals( expected, reader.getTimeStamp() );
    }
}
