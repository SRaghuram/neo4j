/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.causalclustering.catchup.storecopy.StoreFileStreamingProtocol;
import com.neo4j.causalclustering.catchup.storecopy.StoreResource;
import com.neo4j.configuration.CausalClusteringSettings;
import io.netty.channel.ChannelHandlerContext;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;

import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;

class BareFilesHolder
{
    private FileSystemAbstraction fs;
    private StoreFileStreamingProtocol storeFileStreamingProtocol;

    BareFilesHolder()
    {
        var maxChunkSize = CausalClusteringSettings.store_copy_chunk_size.defaultValue();
        this.fs = new EphemeralFileSystemAbstraction();
        this.storeFileStreamingProtocol = new StoreFileStreamingProtocol( maxChunkSize );
    }

    void close()
    {
        try
        {
            fs.close();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    void prepareFile( String fileName, int size ) throws IOException
    {
        var file = Path.of( fileName );
        if ( fs.fileExists( file.toFile() ) )
        {
            fs.deleteFile( file.toFile() );
        }
        var channel = fs.write( file.toFile() );
        var bytes = new byte[size];
        new Random().nextBytes( bytes );
        var buffer = ByteBuffer.wrap( bytes );
        channel.writeAll( buffer );
        channel.flush();
    }

    void sendFile( ChannelHandlerContext ctx, String fileName )
    {
        var file = Path.of( fileName );
        if ( !fs.fileExists( file.toFile() ) )
        {
            throw new IllegalArgumentException( fileName );
        }
        var resource = new StoreResource( file, file.getFileName().toString(), 0, fs );
        storeFileStreamingProtocol.stream( ctx, resource );
    }

    void sendFileWithFileComplete( ChannelHandlerContext ctx, String fileName, long lastTxId )
    {
        sendFile( ctx, fileName );
        storeFileStreamingProtocol.end( ctx, SUCCESS, lastTxId );
    }

    File[] getFiles()
    {
        return fs.listFiles( new File( "." ) );
    }
}
