/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.console.input;

import java.io.PrintStream;

import org.neo4j.helpers.Args;

public abstract class ArgsCommand implements Command
{
    @Override
    public final void run( String[] args, PrintStream out ) throws Exception
    {
        run( Args.parse( args ), out );
    }

    protected abstract void run( Args args, PrintStream out ) throws Exception;
}
