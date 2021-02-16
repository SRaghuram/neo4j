/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.utils;

import java.util.stream.Stream;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

public class ShardFunctions
{
    @Procedure
    @Description( "read procedure" )
    public Stream<ProxyFunctions.Result> reader()
    {
        return Stream.of( new ProxyFunctions.Result( "read" ) );
    }

    @Procedure( mode = Mode.WRITE )
    @Description( "write procedure" )
    public Stream<ProxyFunctions.Result> writer()
    {
        return Stream.of( new ProxyFunctions.Result( "write" ) );
    }

    @Procedure
    @Description( "deprecated return column procedure" )
    public Stream<ProxyFunctions.MultiResult> procWithDepr()
    {
        return Stream.of( new ProxyFunctions.MultiResult( "foo", "bar" ) );
    }

    @Procedure
    @Description( "read procedure on shards only" )
    public Stream<Result> readerOnShard()
    {
        return Stream.of( new Result( "read" ) );
    }

    @Procedure( mode = Mode.WRITE )
    @Description( "write procedure on shards only" )
    public Stream<Result> writerOnShard()
    {
        return Stream.of( new Result( "write" ) );
    }

    @Procedure
    @Description( "void procedure" )
    public void voidProc()
    {
        int answer = 40 + 2;
    }

    public static class Result
    {
        public final String foo;

        public Result( String foo )
        {
            this.foo = foo;
        }
    }
}
