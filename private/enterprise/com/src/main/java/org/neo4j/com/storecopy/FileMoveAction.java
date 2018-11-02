/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com.storecopy;

import java.io.File;
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
    void move( File toDir, CopyOption... copyOptions ) throws IOException;

    File file();

    static FileMoveAction copyViaFileSystem( File file, File basePath )
    {
        Path base = basePath.toPath();
        return new FileMoveAction()
        {
            @Override
            public void move( File toDir, CopyOption... copyOptions ) throws IOException
            {
                Path originalPath = file.toPath();
                Path relativePath = base.relativize( originalPath );
                Path resolvedPath = toDir.toPath().resolve( relativePath );
                if ( !Files.isSymbolicLink( resolvedPath.getParent() ) )
                {
                    Files.createDirectories( resolvedPath.getParent() );
                }
                Files.copy( originalPath, resolvedPath, copyOptions );
            }

            @Override
            public File file()
            {
                return file;
            }
        };
    }

    static FileMoveAction moveViaFileSystem( File sourceFile, File sourceDirectory )
    {
        return new FileMoveAction()
        {
            @Override
            public void move( File toDir, CopyOption... copyOptions ) throws IOException
            {
                copyViaFileSystem( sourceFile, sourceDirectory ).move( toDir, copyOptions );
                if ( !sourceFile.delete() )
                {
                    throw new IOException( "Unable to delete source file after copying " + sourceFile );
                }
            }

            @Override
            public File file()
            {
                return sourceFile;
            }
        };
    }
}
