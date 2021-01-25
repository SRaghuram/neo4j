/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.input;

import java.io.PrintStream;

import org.neo4j.internal.helpers.Args;

public abstract class ArgsCommand implements Command
{
    @Override
    public final void run( String[] args, PrintStream out ) throws Exception
    {
        run( Args.parse( args ), out );
    }

    protected abstract void run( Args args, PrintStream out ) throws Exception;
}
