/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.fs.ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;

@ExtendWith( EphemeralFileSystemExtension.class )
class ReadAheadChannelTest
{
    @Inject
    protected EphemeralFileSystemAbstraction fileSystem;

    @ParameterizedTest
    @EnumSource( Constructors.class )
    void shouldThrowExceptionForReadAfterEOFIfNotEnoughBytesExist( Constructor constructor ) throws Exception
    {
        // Given
        File bytesReadTestFile = new File( "bytesReadTest.txt" );
        StoreChannel storeChannel = fileSystem.write( bytesReadTestFile );
        ByteBuffer buffer = ByteBuffer.allocate( 1 );
        buffer.put( (byte) 1 );
        buffer.flip();
        storeChannel.writeAll( buffer );
        storeChannel.force( false );
        storeChannel.close();

        storeChannel = fileSystem.read( bytesReadTestFile );

        ReadAheadChannel<StoreChannel> channel = constructor.apply( storeChannel, DEFAULT_READ_AHEAD_SIZE );
        assertEquals( (byte) 1, channel.get() );

        assertThrows( ReadPastEndException.class, channel::get );
        assertThrows( ReadPastEndException.class, channel::get );
    }

    @ParameterizedTest
    @EnumSource( Constructors.class )
    void shouldReturnValueIfSufficientBytesAreBufferedEvenIfEOFHasBeenEncountered( Constructor constructor ) throws Exception
    {
        // Given
        File shortReadTestFile = new File( "shortReadTest.txt" );
        StoreChannel storeChannel = fileSystem.write( shortReadTestFile );
        ByteBuffer buffer = ByteBuffer.allocate( 1 );
        buffer.put( (byte) 1 );
        buffer.flip();
        storeChannel.writeAll( buffer );
        storeChannel.force( false );
        storeChannel.close();

        storeChannel = fileSystem.read( shortReadTestFile );
        ReadAheadChannel<StoreChannel> channel = constructor.apply( storeChannel, DEFAULT_READ_AHEAD_SIZE );

        assertThrows( ReadPastEndException.class, channel::getShort );
        assertEquals( (byte) 1, channel.get() );
        assertThrows( ReadPastEndException.class, channel::get );
    }

    @ParameterizedTest
    @EnumSource( Constructors.class )
    void shouldHandleRunningOutOfBytesWhenRequestSpansMultipleFiles( Constructor constructor ) throws Exception
    {
        // Given
        StoreChannel storeChannel1 = fileSystem.write( new File( "foo.1" ) );
        ByteBuffer buffer = ByteBuffer.allocate( 2 );
        buffer.put( (byte) 0 );
        buffer.put( (byte) 0 );
        buffer.flip();
        storeChannel1.writeAll( buffer );
        storeChannel1.force( false );
        storeChannel1.close();

        buffer.flip();

        StoreChannel storeChannel2 = fileSystem.read( new File( "foo.2" ) );
        buffer.put( (byte) 0 );
        buffer.put( (byte) 1 );
        buffer.flip();
        storeChannel2.writeAll( buffer );
        storeChannel2.force( false );
        storeChannel2.close();

        storeChannel1 = fileSystem.read( new File( "foo.1" ) );
        final StoreChannel storeChannel2Copy = fileSystem.read( new File( "foo.2" ) );

        HookedReadAheadChannel channel = constructor.apply( storeChannel1, DEFAULT_READ_AHEAD_SIZE );
        channel.nextChannelHook = storeChannel2Copy;

        assertThrows( ReadPastEndException.class, channel::getLong );
        assertEquals( 1, channel.getInt() );
        assertThrows( ReadPastEndException.class, channel::get );
    }

    @ParameterizedTest
    @EnumSource( Constructors.class )
    void shouldReturnPositionWithinBufferedStream( Constructor constructor ) throws Exception
    {
        // given
        File file = new File( "foo.txt" );

        int readAheadSize = 512;
        int fileSize = readAheadSize * 8;

        createFile( fileSystem, file, fileSize );
        ReadAheadChannel<StoreChannel> bufferedReader = constructor.apply( fileSystem.read( file ), readAheadSize );

        // when
        for ( int i = 0; i < fileSize / Long.BYTES; i++ )
        {
            assertEquals( Long.BYTES * i, bufferedReader.position() );
            bufferedReader.getLong();
        }

        assertEquals( fileSize, bufferedReader.position() );
        assertThrows( ReadPastEndException.class, bufferedReader::getLong );
        assertEquals( fileSize, bufferedReader.position() );
    }

    private void createFile( EphemeralFileSystemAbstraction fsa, File name, int bufferSize ) throws IOException
    {
        StoreChannel storeChannel = fsa.write( name );
        ByteBuffer buffer = ByteBuffer.allocate( bufferSize );
        for ( int i = 0; i < bufferSize; i++ )
        {
            buffer.put( (byte) i );
        }
        buffer.flip();
        storeChannel.writeAll( buffer );
        storeChannel.close();
    }

    private static class HookedReadAheadChannel extends ReadAheadChannel<StoreChannel>
    {
        StoreChannel nextChannelHook;

        HookedReadAheadChannel( StoreChannel channel, int readAheadSize )
        {
            super( channel, readAheadSize );
        }

        HookedReadAheadChannel( StoreChannel channel, ByteBuffer byteBuffer )
        {
            super( channel, byteBuffer );
        }

        @Override
        protected StoreChannel next( StoreChannel channel ) throws IOException
        {
            if ( nextChannelHook != null )
            {
                StoreChannel next = nextChannelHook;
                nextChannelHook = null;
                return next;
            }
            return super.next( channel );
        }
    }

    interface Constructor
    {
        HookedReadAheadChannel apply( StoreChannel channel, int readAheadSize );
    }

    enum Constructors implements Constructor
    {
        HEAP_BUFFER
                {
                    @Override
                    public HookedReadAheadChannel apply( StoreChannel channel, int readAheadSize )
                    {
                        return new HookedReadAheadChannel( channel, ByteBuffer.allocate( readAheadSize ) );
                    }
                },
        DIRECT_BUFFER
                {
                    @Override
                    public HookedReadAheadChannel apply( StoreChannel channel, int readAheadSize )
                    {
                        return new HookedReadAheadChannel( channel, ByteBuffer.allocateDirect( readAheadSize ) );
                    }
                },
        INNER_BUFFER
                {
                    @Override
                    public HookedReadAheadChannel apply( StoreChannel channel, int readAheadSize )
                    {
                        return new HookedReadAheadChannel( channel, readAheadSize );
                    }
                }
    }
}
