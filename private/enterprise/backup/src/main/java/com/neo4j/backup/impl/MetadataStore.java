/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MetadataStore
{
    private static final String METADATA_NAME = "metadata_script.cypher";
    private static final Charset CHARSET = UTF_8;

    private final FileSystemAbstraction fs;

    public MetadataStore( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    public static Path getFilePath( Path backupFolder )
    {
        return backupFolder.resolve( METADATA_NAME );
    }

    public static boolean isMetadataFile( Path path )
    {
        final var file = path.toFile();
        return file.exists() && file.isFile() && path.getFileName().toString().equals( METADATA_NAME );
    }

    public void write( Path backupFolder, List<String> values ) throws IOException
    {
        final var filePath = getFilePath( backupFolder );

        final var value = String.join( ";\n", values ) + ";"; // Cypher queries must end with a semicolon
        try ( Writer writer = fs.openAsWriter( filePath, CHARSET, false ) )
        {
            write( writer, value );
        }
    }

    private void write( Writer writer, String value )
    {
        try
        {
            writer.append( value );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
