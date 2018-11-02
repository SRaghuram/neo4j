/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.console.input;

import java.io.PrintStream;

/**
 * Action to be run for a specific command, read from {@link ConsoleInput}.
 */
public interface Command
{
    void run( String[] args, PrintStream out ) throws Exception;
}
