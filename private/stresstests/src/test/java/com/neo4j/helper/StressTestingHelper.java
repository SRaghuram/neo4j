/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.helper;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.fs.FileUtils;

import static java.lang.System.getenv;
import static java.nio.file.Files.createDirectories;

public final class StressTestingHelper
{
    private StressTestingHelper()
    {
    }

    public static Path ensureExistsAndEmpty( Path directory ) throws IOException
    {
        FileUtils.deleteDirectory( directory );
        createDirectories( directory );
        return directory;
    }

    public static String fromEnv( String environmentVariableName, String defaultValue )
    {
        String environmentVariableValue = getenv( environmentVariableName );
        return environmentVariableValue == null ? defaultValue : environmentVariableValue;
    }
}
