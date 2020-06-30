/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.helper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.fs.FileUtils;

import static java.lang.System.getenv;
import static java.nio.file.Files.createDirectories;
import static org.neo4j.io.fs.FileUtils.deletePathRecursively;

public final class StressTestingHelper
{
    private StressTestingHelper()
    {
    }

    public static File ensureExistsAndEmpty( File directory ) throws IOException
    {
        FileUtils.deleteRecursively( directory );

        if ( !directory.mkdirs() )
        {
            throw new RuntimeException( "Could not create directory: " + directory.getAbsolutePath() );
        }
        return directory;
    }

    public static Path ensureExistsAndEmpty( Path directory ) throws IOException
    {
        deletePathRecursively( directory );
        createDirectories( directory );
        return directory;
    }

    public static String fromEnv( String environmentVariableName, String defaultValue )
    {
        String environmentVariableValue = getenv( environmentVariableName );
        return environmentVariableValue == null ? defaultValue : environmentVariableValue;
    }
}
