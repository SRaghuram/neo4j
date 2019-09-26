/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.helpers.Buffers;
import io.netty.buffer.ByteBuf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.Random;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static com.neo4j.causalclustering.catchup.storecopy.FileChunk.MAX_PAYLOAD_SIZE;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileSenderTest
{
    private final Random random = new Random();
    @Rule
    public final Buffers allocator = new Buffers();
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final FileSystemAbstraction fs = fsRule.get();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( fsRule.get() );
    private PageCache pageCache = mock( PageCache.class );

    @Before
    public void setup() throws IOException
    {
        when( pageCache.getExistingMapping( any() ) ).thenReturn( Optional.empty() );
    }

    @Test
    public void sendEmptyFile() throws Exception
    {
        // given
        File emptyFile = testDirectory.file( "emptyFile" );
        fs.write( emptyFile ).close();
        FileSender fileSender = new FileSender( new StoreResource( emptyFile, null, 16, fs ) );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertEquals( FileChunk.create( EMPTY_BUFFER, true ), fileSender.readChunk( allocator ) );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    public void sendSmallFile() throws Exception
    {
        // given
        ByteBuf buffer = getRandomBuffer( 10 );

        File smallFile = testDirectory.file( "smallFile" );
        try ( StoreChannel channel = fs.write( smallFile ) )
        {
            buffer.readBytes( channel, buffer.readableBytes() );
        }

        FileSender fileSender = new FileSender( new StoreResource( smallFile, null, 16, fs ) );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertNextChunkEquals( fileSender, buffer, 0, 10, true );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    public void sendLargeFile() throws Exception
    {
        // given
        int totalSize = MAX_PAYLOAD_SIZE + (MAX_PAYLOAD_SIZE / 2);
        ByteBuf buffer = getRandomBuffer( totalSize );

        File smallFile = testDirectory.file( "smallFile" );
        try ( StoreChannel channel = fs.write( smallFile ) )
        {
            buffer.readBytes( channel, buffer.readableBytes() );
        }

        FileSender fileSender = new FileSender( new StoreResource( smallFile, null, 16, fs ) );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertNextChunkEquals( fileSender, buffer, 0, MAX_PAYLOAD_SIZE, false );
        assertNextChunkEquals( fileSender, buffer, MAX_PAYLOAD_SIZE, MAX_PAYLOAD_SIZE / 2, true );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    public void sendLargeFileWithSizeMultipleOfTheChunkSize() throws Exception
    {
        // given
        ByteBuf buffer = getRandomBuffer( MAX_PAYLOAD_SIZE * 3 );

        File smallFile = testDirectory.file( "smallFile" );
        try ( StoreChannel channel = fs.write( smallFile ) )
        {
            buffer.readBytes( channel, buffer.readableBytes() );
        }

        FileSender fileSender = new FileSender( new StoreResource( smallFile, null, 16, fs ) );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertNextChunkEquals( fileSender, buffer, 0, MAX_PAYLOAD_SIZE, false );
        assertNextChunkEquals( fileSender, buffer, MAX_PAYLOAD_SIZE, MAX_PAYLOAD_SIZE, false );
        assertNextChunkEquals( fileSender, buffer, MAX_PAYLOAD_SIZE * 2, MAX_PAYLOAD_SIZE, true );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    public void sendEmptyFileWhichGrowsBeforeSendCommences() throws Exception
    {
        // given
        File file = testDirectory.file( "file" );
        StoreChannel channel = fs.write( file );
        FileSender fileSender = new FileSender( new StoreResource( file, null, 16, fs ) );

        // when
        ByteBuf buffer = writeRandomBytes( channel, 1024 );

        // then
        assertFalse( fileSender.isEndOfInput() );
        assertNextChunkEquals( fileSender, buffer, 0, 1024, true );
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    public void sendEmptyFileWhichGrowsWithPartialChunkSizes() throws Exception
    {
        // given
        File file = testDirectory.file( "file" );
        StoreChannel channel = fs.write( file );
        FileSender fileSender = new FileSender( new StoreResource( file, null, 16, fs ) );

        // when
        ByteBuf chunkA = writeRandomBytes( channel, MAX_PAYLOAD_SIZE );
        ByteBuf chunkB = writeRandomBytes( channel, MAX_PAYLOAD_SIZE / 2 );

        // then
        assertNextChunkEquals( fileSender, chunkA, 0, MAX_PAYLOAD_SIZE, false );
        assertFalse( fileSender.isEndOfInput() );

        // when
        writeRandomBytes( channel, MAX_PAYLOAD_SIZE / 2 );

        // then
        assertNextChunkEquals( fileSender, chunkB, 0, MAX_PAYLOAD_SIZE / 2, true );
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    public void sendFileWhichGrowsAfterLastChunkWasSent() throws Exception
    {
        // given
        File file = testDirectory.file( "file" );
        StoreChannel channel = fs.write( file );
        FileSender fileSender = new FileSender( new StoreResource( file, null, 16, fs ) );

        // when
        ByteBuf chunkA = writeRandomBytes( channel, MAX_PAYLOAD_SIZE );

        // then
        assertNextChunkEquals( fileSender, chunkA, 0, MAX_PAYLOAD_SIZE, true );
        assertTrue( fileSender.isEndOfInput() );

        // when
        writeRandomBytes( channel, MAX_PAYLOAD_SIZE );

        // then
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    public void sendLargerFileWhichGrows() throws Exception
    {
        // given
        File file = testDirectory.file( "file" );
        StoreChannel channel = fs.write( file );
        FileSender fileSender = new FileSender( new StoreResource( file, null, 16, fs ) );

        // when
        ByteBuf chunkA = writeRandomBytes( channel, MAX_PAYLOAD_SIZE );
        ByteBuf chunkB = writeRandomBytes( channel, MAX_PAYLOAD_SIZE );

        // then
        assertNextChunkEquals( fileSender, chunkA, 0, MAX_PAYLOAD_SIZE, false );
        assertFalse( fileSender.isEndOfInput() );

        // when
        ByteBuf chunkC = writeRandomBytes( channel, MAX_PAYLOAD_SIZE );

        // then
        assertNextChunkEquals( fileSender, chunkB, 0, MAX_PAYLOAD_SIZE, false );
        assertFalse( fileSender.isEndOfInput() );

        // when
        assertNextChunkEquals( fileSender, chunkC, 0, MAX_PAYLOAD_SIZE, true );

        // then
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    public void sendLargeFileWithUnreliableReadBufferSize() throws Exception
    {
        // given
        ByteBuf buffer = getRandomBuffer( MAX_PAYLOAD_SIZE * 3 );

        File smallFile = testDirectory.file( "smallFile" );
        try ( StoreChannel channel = fs.write( smallFile ) )
        {
            buffer.readBytes( channel, buffer.readableBytes() );
        }

        Adversary adversary = new RandomAdversary( 0.9, 0.0, 0.0 );
        AdversarialFileSystemAbstraction afs = new AdversarialFileSystemAbstraction( adversary, fs );
        FileSender fileSender = new FileSender( new StoreResource( smallFile, null, 16, afs ) );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertNextChunkEquals( fileSender, buffer, 0, MAX_PAYLOAD_SIZE, false );
        assertNextChunkEquals( fileSender, buffer, MAX_PAYLOAD_SIZE, MAX_PAYLOAD_SIZE, false );
        assertNextChunkEquals( fileSender, buffer, MAX_PAYLOAD_SIZE * 2, MAX_PAYLOAD_SIZE, true );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    private void assertNextChunkEquals( FileSender fileSender, ByteBuf expected, int startIndex, int length, boolean isLast ) throws Exception
    {
        assertEquals( FileChunk.create( expected.slice( startIndex, length ), isLast ), fileSender.readChunk( allocator ) );
    }

    private ByteBuf getRandomBuffer( int numberOfBytes )
    {
        byte[] bytes = new byte[numberOfBytes];
        random.nextBytes( bytes );
        ByteBuf buffer = allocator.buffer( numberOfBytes );
        return buffer.writeBytes( bytes );
    }

    private ByteBuf writeRandomBytes( StoreChannel channel, int numberOfBytes ) throws IOException
    {
        ByteBuf buffer = getRandomBuffer( numberOfBytes );
        channel.writeAll( buffer.nioBuffer() );
        return buffer;
    }
}
