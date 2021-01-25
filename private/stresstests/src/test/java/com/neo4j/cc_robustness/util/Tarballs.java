/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;

import org.neo4j.logging.Log;

public class Tarballs
{
    public static void targz( Path input, Path output, Log log )
    {
        try
        {
            String targetFileName = output.toString();
            Process process = Runtime.getRuntime().exec( new String[]{"tar", "zcf", targetFileName, input.toString()} );
            readAndPrintOutputFrom( process, log );
            int result = process.waitFor();
            if ( result != 0 )
            {
                throw new RuntimeException( "Couldn't pack db '" + input + "' to '" + targetFileName + "'" );
            }
            log.info( "Packed it --> " + output );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new RuntimeException( e );
        }
    }

    private static void readAndPrintOutputFrom( Process process, Log log ) throws IOException
    {
        exhaustAndPrintStream( process.getInputStream(), log );
        exhaustAndPrintStream( process.getErrorStream(), log );
    }

    private static void exhaustAndPrintStream( InputStream inputStream, Log log ) throws IOException
    {
        try ( inputStream )
        {
            BufferedReader reader = new BufferedReader( new InputStreamReader( inputStream ) );
            String line;
            while ( (line = reader.readLine()) != null )
            {
                log.info( line );
            }
        }
    }
}
