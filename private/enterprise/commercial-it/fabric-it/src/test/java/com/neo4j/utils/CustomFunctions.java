/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.utils;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class CustomFunctions
{
    @UserFunction
    @Description( "adds one" )
    public long myPlusOne( @Name( "value" ) long value )
    {
        return value + 1;
    }

    @UserFunction
    @Description( "shard id for uid" )
    public long personShard( @Name( "uid" ) long uid )
    {
        return uid / 100;
    }
}
