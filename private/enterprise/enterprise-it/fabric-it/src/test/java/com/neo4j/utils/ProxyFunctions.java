/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.utils;

import java.util.stream.Stream;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

public class ProxyFunctions
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

    @Procedure
    @Description( "read procedure" )
    public Stream<Result> reader()
    {
        return Stream.of( new Result( "read" ) );
    }

    @Procedure( mode = Mode.WRITE )
    @Description( "write procedure" )
    public Stream<Result> writer()
    {
        return Stream.of( new Result( "write" ) );
    }

    @Procedure
    @Description( "deprecated return column procedure" )
    public Stream<MultiResult> procWithDepr()
    {
        return Stream.of( new MultiResult( "foo", "bar" ) );
    }

    public static class Result
    {
        public final String foo;

        public Result( String foo )
        {
            this.foo = foo;
        }
    }

    public static class MultiResult
    {
        public final String foo;
        @Deprecated( since = "For testing deprecated return column for procedures" )
        public final String bar;

        public MultiResult( String foo, String bar )
        {
            this.foo = foo;
            this.bar = bar;
        }
    }
}
