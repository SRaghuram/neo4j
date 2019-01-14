/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.test.extension.SuppressOutputExtension;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( SuppressOutputExtension.class )
class DumpStoreTest
{
    @Test
    void dumpStoreShouldPrintBufferWithContent()
    {
        // Given
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        DumpStore dumpStore = new DumpStore( out );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        for ( byte i = 0; i < 10; i++ )
        {
            buffer.put( i );
        }
        buffer.flip();

        AbstractBaseRecord record = Mockito.mock( AbstractBaseRecord.class );

        // When
        //when( record.inUse() ).thenReturn( true );
        dumpStore.dumpHex( record, buffer, 2, 4 );

        // Then
        assertEquals( format( "@ 0x00000008: 00 01 02 03  04 05 06 07  08 09%n" ), outStream.toString() );
    }

    @Test
    void dumpStoreShouldPrintShorterMessageForAllZeroBuffer()
    {
        // Given
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        DumpStore dumpStore = new DumpStore( out );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        AbstractBaseRecord record = Mockito.mock( AbstractBaseRecord.class );

        // When
        //when( record.inUse() ).thenReturn( true );
        dumpStore.dumpHex( record, buffer, 2, 4 );

        // Then
        assertEquals( format( ": all zeros @ 0x8 - 0xc%n" ), outStream.toString() );
    }

    @Test
    void canDumpNeoStoreFileContent() throws Exception
    {
        URL neostore = getClass().getClassLoader().getResource( "neostore" );
        String neostoreFile = neostore.getFile();
        DumpStore.main( neostoreFile );
    }
}
