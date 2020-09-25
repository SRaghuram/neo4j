/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DatabaseIdStore
{
    private static final int UUID_BYTE_LENGTH = 36;
    public static final String FILE_NAME = "database.id";
    static final Charset CHARSET = UTF_8;

    private final FileSystemAbstraction fs;
    private final Log log;

    public DatabaseIdStore( FileSystemAbstraction fs, LogProvider logProvider )
    {
        this.fs = fs;
        this.log = logProvider.getLog( DatabaseIdStore.class );
    }

    public void writeDatabaseId( DatabaseId databaseId, Path folderPath ) throws IOException
    {
        final var filePath = getDatabaseFilePath( folderPath );
        try ( StoreChannel channel = fs.write( filePath ) )
        {
            channel.writeAll( ByteBuffer.wrap( databaseId.uuid().toString().getBytes( CHARSET ) ) );
        }
    }

    public Optional<DatabaseId> readDatabaseId( Path folderPath )
    {
        final var filePath = getDatabaseFilePath( folderPath );
        try ( StoreChannel channel = fs.read( filePath ) )
        {
            final var buffer = ByteBuffer.allocate( UUID_BYTE_LENGTH );
            channel.readAll( buffer );
            byte[] bytes = new byte[UUID_BYTE_LENGTH];
            buffer.flip().get( bytes );
            final var uuid = UUID.fromString( new String( bytes, UTF_8 ) );
            return Optional.of( DatabaseIdFactory.from( uuid ) );
        }
        catch ( Exception exception )
        {
            log.error( "Error in reading database id from path={}", filePath, exception );
        }
        return Optional.empty();
    }

    private Path getDatabaseFilePath( Path folderPath )
    {
        return folderPath.resolve( FILE_NAME );
    }
}
