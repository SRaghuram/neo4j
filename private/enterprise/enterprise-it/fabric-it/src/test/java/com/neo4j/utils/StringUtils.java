/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.utils;

public class StringUtils
{
    public static String lines( String... lines )
    {
        return String.join( System.lineSeparator(), lines );
    }
}
