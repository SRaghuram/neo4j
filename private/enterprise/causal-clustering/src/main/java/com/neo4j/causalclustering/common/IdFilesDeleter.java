/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;

public class IdFilesDeleter
{
    public static boolean deleteIdFiles( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem )
    {
        if ( !fileSystem.fileExists( databaseLayout.databaseDirectory() ) )
        {
            return false;
        }

        boolean anyIdFilesDeleted = false;
        for ( File idFile : databaseLayout.idFiles() )
        {
            try
            {
                if ( fileSystem.fileExists( idFile ) )
                {
                    fileSystem.deleteFileOrThrow( idFile );
                }
                anyIdFilesDeleted = true;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Could not delete ID-file", e );
            }
        }

        return anyIdFilesDeleted;
    }
}
