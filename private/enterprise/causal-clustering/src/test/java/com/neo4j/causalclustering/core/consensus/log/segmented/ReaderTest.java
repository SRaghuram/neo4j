/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReaderTest
{
    private final FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
    private final StoreChannel channel = mock( StoreChannel.class );
    private final Path file = mock( Path.class );

    @Test
    void shouldCloseChannelOnClose() throws Exception
    {
        // given
        when( fsa.read( file.toFile() ) ).thenReturn( channel );
        Reader reader = new Reader( fsa, file, 0 );

        // when
        reader.close();

        // then
        verify( channel ).close();
    }

    @Test
    void shouldUpdateTimeStamp() throws Exception
    {
        // given
        Reader reader = new Reader( fsa, file, 0 );

        // when
        int expected = 123;
        reader.setTimeStamp( expected );

        // then
        Assertions.assertEquals( expected, reader.getTimeStamp() );
    }
}
