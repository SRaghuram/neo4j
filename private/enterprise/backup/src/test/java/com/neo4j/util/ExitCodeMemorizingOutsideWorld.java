/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.util;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.commandline.admin.RealOutsideWorld;

class ExitCodeMemorizingOutsideWorld extends RealOutsideWorld
{
    private int exitCode = -1;

    @Override
    public void exit( int status )
    {
        close();
        exitCode = status;
    }

    @Override
    public void close()
    {
        try
        {
            super.close();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    int getExitCode()
    {
        return exitCode;
    }
}
