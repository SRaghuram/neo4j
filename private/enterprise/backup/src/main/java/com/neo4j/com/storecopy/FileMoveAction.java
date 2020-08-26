/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.com.storecopy;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;

public interface FileMoveAction
{
    /**
     * Execute a file move, moving the prepared file to the given {@code toDir}.
     * @param toDir The target directory of the move operation
     * @param copyOptions
     * @throws IOException
     */
    void move( Path toDir, CopyOption... copyOptions ) throws IOException;

    Path file();

    static FileMoveAction copyViaFileSystem( Path file, Path basePath )
    {
        return new FileMoveAction()
        {
            @Override
            public void move( Path toDir, CopyOption... copyOptions ) throws IOException
            {
                Path relativePath = basePath.relativize( file );
                Path resolvedPath = toDir.resolve( relativePath );
                if ( !Files.isSymbolicLink( resolvedPath.getParent() ) )
                {
                    Files.createDirectories( resolvedPath.getParent() );
                }
                Files.copy( file, resolvedPath, copyOptions );
            }

            @Override
            public Path file()
            {
                return file;
            }
        };
    }

    static FileMoveAction moveViaFileSystem( Path sourceFile, Path sourceDirectory )
    {
        return new FileMoveAction()
        {
            @Override
            public void move( Path toDir, CopyOption... copyOptions ) throws IOException
            {
                copyViaFileSystem( sourceFile, sourceDirectory ).move( toDir, copyOptions );
                Files.delete( sourceFile );
            }

            @Override
            public Path file()
            {
                return sourceFile;
            }
        };
    }
}
