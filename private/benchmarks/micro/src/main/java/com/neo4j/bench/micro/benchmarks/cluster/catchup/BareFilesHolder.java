/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.causalclustering.catchup.storecopy.StoreFileStreamingProtocol;
import com.neo4j.causalclustering.catchup.storecopy.StoreResource;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import io.netty.channel.ChannelHandlerContext;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        var file = new File( fileName );
        if ( fs.fileExists( file ) )
        {
            fs.deleteFile( file );
        }
        var channel = fs.write( file );
        var bytes = new byte[size];
        new Random().nextBytes( bytes );
        var buffer = ByteBuffer.wrap( bytes );
        channel.writeAll( buffer );
        channel.flush();
    }

    void sendFile( ChannelHandlerContext ctx, String fileName )
    {
        var file = new File( fileName );
        if ( !fs.fileExists( file ) )
        {
            throw new IllegalArgumentException( fileName );
        }
        var resource = new StoreResource( file, file.getName(), 0, fs );
        storeFileStreamingProtocol.stream( ctx, resource );
    }

    void sendFileWithFileComplete( ChannelHandlerContext ctx, String fileName, long lastTxId )
    {
        sendFile( ctx, fileName );
        storeFileStreamingProtocol.end( ctx, SUCCESS, lastTxId );
    }

    List<File> getFiles()
    {
        return Stream.of( fs.listFiles( new File( "." ) ) ).collect( Collectors.toList() );
    }
}
