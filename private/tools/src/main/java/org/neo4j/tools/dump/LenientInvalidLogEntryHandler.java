/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump;

import java.io.PrintStream;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;

/**
 * Is less strict with invalid log entries, allowing reader to try and read through bad sections.
 * Prints problems along the way.
 */
class LenientInvalidLogEntryHandler extends InvalidLogEntryHandler
{
    private final PrintStream out;

    LenientInvalidLogEntryHandler( PrintStream out )
    {
        this.out = out;
    }

    @Override
    public boolean handleInvalidEntry( Exception e, LogPosition position )
    {
        out.println( "Read broken entry with error:" + e + ", will go one byte ahead and try again" );
        return true;
    }

    @Override
    public void bytesSkipped( long bytesSkipped )
    {
        out.println( "... skipped " + bytesSkipped + " bytes of indecipherable transaction log data" );
    }
}
