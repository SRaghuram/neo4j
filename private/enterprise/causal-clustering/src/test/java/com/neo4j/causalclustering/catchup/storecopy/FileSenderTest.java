/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.helpers.Buffers;
import com.neo4j.causalclustering.helpers.BuffersExtension;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Optional;
import java.util.Random;

import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectorySupportExtension.class, BuffersExtension.class} )
class FileSenderTest
{
    private final Random random = new Random();

    @Inject
    private Buffers allocator;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private PageCache pageCache = mock( PageCache.class );
    private int maxChunkSize = 32768;

    @BeforeEach
    void setup() throws IOException
    {
        when( pageCache.getExistingMapping( any() ) ).thenReturn( Optional.empty() );
    }

    @Test
    void sendEmptyFile() throws Exception
    {
        // given
        var emptyFile = testDirectory.filePath( "emptyFile" );
        fs.write( emptyFile ).close();
        var fileSender = new FileSender( new StoreResource( emptyFile, null, 16, fs ), maxChunkSize );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertEquals( FileChunk.create( EMPTY_BUFFER, true, maxChunkSize ), fileSender.readChunk( allocator ) );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    void sendSmallFile() throws Exception
    {
        // given
        var buffer = getRandomBuffer( 10 );

        var smallFile = testDirectory.filePath( "smallFile" );
        try ( var channel = fs.write( smallFile ) )
        {
            buffer.readBytes( channel, buffer.readableBytes() );
        }

        var fileSender = new FileSender( new StoreResource( smallFile, null, 16, fs ), maxChunkSize );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertNextChunkEquals( fileSender, buffer, 0, 10, true );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    void sendLargeFile() throws Exception
    {
        // given
        var totalSize = maxChunkSize + (maxChunkSize / 2);
        var buffer = getRandomBuffer( totalSize );

        var smallFile = testDirectory.filePath( "smallFile" );
        try ( var channel = fs.write( smallFile ) )
        {
            buffer.readBytes( channel, buffer.readableBytes() );
        }

        var fileSender = new FileSender( new StoreResource( smallFile, null, 16, fs ), maxChunkSize );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertNextChunkEquals( fileSender, buffer, 0, maxChunkSize, false );
        assertNextChunkEquals( fileSender, buffer, maxChunkSize, maxChunkSize / 2, true );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    void sendLargeFileWithSizeMultipleOfTheChunkSize() throws Exception
    {
        // given
        var buffer = getRandomBuffer( maxChunkSize * 3 );

        var smallFile = testDirectory.filePath( "smallFile" );
        try ( var channel = fs.write( smallFile ) )
        {
            buffer.readBytes( channel, buffer.readableBytes() );
        }

        var fileSender = new FileSender( new StoreResource( smallFile, null, 16, fs ), maxChunkSize );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertNextChunkEquals( fileSender, buffer, 0, maxChunkSize, false );
        assertNextChunkEquals( fileSender, buffer, maxChunkSize, maxChunkSize, false );
        assertNextChunkEquals( fileSender, buffer, maxChunkSize * 2, maxChunkSize, true );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    void sendEmptyFileWhichGrowsBeforeSendCommences() throws Exception
    {
        // given
        var file = testDirectory.filePath( "file" );
        var channel = fs.write( file );
        var fileSender = new FileSender( new StoreResource( file, null, 16, fs ), maxChunkSize );

        // when
        var buffer = writeRandomBytes( channel, 1024 );

        // then
        assertFalse( fileSender.isEndOfInput() );
        assertNextChunkEquals( fileSender, buffer, 0, 1024, true );
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    void sendEmptyFileWhichGrowsWithPartialChunkSizes() throws Exception
    {
        // given
        var file = testDirectory.filePath( "file" );
        var channel = fs.write( file );
        var fileSender = new FileSender( new StoreResource( file, null, 16, fs ), maxChunkSize );

        // when
        var chunkA = writeRandomBytes( channel, maxChunkSize );
        var chunkB = writeRandomBytes( channel, maxChunkSize / 2 );

        // then
        assertNextChunkEquals( fileSender, chunkA, 0, maxChunkSize, false );
        assertFalse( fileSender.isEndOfInput() );

        // when
        writeRandomBytes( channel, maxChunkSize / 2 );

        // then
        assertNextChunkEquals( fileSender, chunkB, 0, maxChunkSize / 2, true );
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    void sendFileWhichGrowsAfterLastChunkWasSent() throws Exception
    {
        // given
        var file = testDirectory.filePath( "file" );
        var channel = fs.write( file );
        var fileSender = new FileSender( new StoreResource( file, null, 16, fs ), maxChunkSize );

        // when
        var chunkA = writeRandomBytes( channel, maxChunkSize );

        // then
        assertNextChunkEquals( fileSender, chunkA, 0, maxChunkSize, true );
        assertTrue( fileSender.isEndOfInput() );

        // when
        writeRandomBytes( channel, maxChunkSize );

        // then
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    void sendLargerFileWhichGrows() throws Exception
    {
        // given
        var file = testDirectory.filePath( "file" );
        var channel = fs.write( file );
        var fileSender = new FileSender( new StoreResource( file, null, 16, fs ), maxChunkSize );

        // when
        var chunkA = writeRandomBytes( channel, maxChunkSize );
        var chunkB = writeRandomBytes( channel, maxChunkSize );

        // then
        assertNextChunkEquals( fileSender, chunkA, 0, maxChunkSize, false );
        assertFalse( fileSender.isEndOfInput() );

        // when
        var chunkC = writeRandomBytes( channel, maxChunkSize );

        // then
        assertNextChunkEquals( fileSender, chunkB, 0, maxChunkSize, false );
        assertFalse( fileSender.isEndOfInput() );

        // when
        assertNextChunkEquals( fileSender, chunkC, 0, maxChunkSize, true );

        // then
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    void sendLargeFileWithUnreliableReadBufferSize() throws Exception
    {
        // given
        var buffer = getRandomBuffer( maxChunkSize * 3 );

        var smallFile = testDirectory.filePath( "smallFile" );
        try ( var channel = fs.write( smallFile ) )
        {
            buffer.readBytes( channel, buffer.readableBytes() );
        }

        var adversary = new RandomAdversary( 0.9, 0.0, 0.0 );
        var afs = new AdversarialFileSystemAbstraction( adversary, fs );
        var fileSender = new FileSender( new StoreResource( smallFile, null, 16, afs ), maxChunkSize );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertNextChunkEquals( fileSender, buffer, 0, maxChunkSize, false );
        assertNextChunkEquals( fileSender, buffer, maxChunkSize, maxChunkSize, false );
        assertNextChunkEquals( fileSender, buffer, maxChunkSize * 2, maxChunkSize, true );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    private void assertNextChunkEquals( FileSender fileSender, ByteBuf expected, int startIndex, int length, boolean isLast ) throws Exception
    {
        assertEquals( FileChunk.create( expected.slice( startIndex, length ), isLast, maxChunkSize ), fileSender.readChunk( allocator ) );
    }

    private ByteBuf getRandomBuffer( int numberOfBytes )
    {
        var bytes = new byte[numberOfBytes];
        random.nextBytes( bytes );
        ByteBuf buffer = allocator.buffer( numberOfBytes );
        return buffer.writeBytes( bytes );
    }

    private ByteBuf writeRandomBytes( StoreChannel channel, int numberOfBytes ) throws IOException
    {
        var buffer = getRandomBuffer( numberOfBytes );
        channel.writeAll( buffer.nioBuffer() );
        return buffer;
    }
}
