/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.util;

import org.neo4j.cli.AdminTool;

/**
 * This is simply calling {@link AdminTool}, but in a place where all commands are available. The class name is also more like the actual tool name.
 * So this is a convenience for running neo4j-admin commands in your IDE.
 */
public class Neo4jAdmin
{
    public static void main( String[] args )
    {
        // Remember to set environment variables in your launch spec:
        // - NEO4J_HOME: home of your dbms
        // - NEO4J_CONF: e.g. <NEO4J_HOME>/conf

        AdminTool.main( args );
    }
}
