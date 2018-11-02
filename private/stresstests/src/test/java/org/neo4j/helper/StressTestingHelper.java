/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.helper;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileUtils;

import static java.lang.System.getenv;

public class StressTestingHelper
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

    public static String fromEnv( String environmentVariableName, String defaultValue )
    {
        String environmentVariableValue = getenv( environmentVariableName );
        return environmentVariableValue == null ? defaultValue : environmentVariableValue;
    }
}
