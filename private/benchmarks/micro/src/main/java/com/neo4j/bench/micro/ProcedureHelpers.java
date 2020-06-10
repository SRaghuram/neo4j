/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

/**
 * Contains utilities used when benchmarking procedures
 */
public final class ProcedureHelpers
{
    private ProcedureHelpers()
    {
        throw new UnsupportedOperationException( "don't instantiate" );
    }

    public static class LongResult
    {
        public Long value;

        public LongResult( Long value )
        {
            this.value = value;
        }
    }

    public static class CypherResult
    {
        public Map<String,Object> value;

        public CypherResult( Map<String,Object> value )
        {
            this.value = value;
        }
    }

    public static class TestProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Transaction tx;

        @Procedure( name = "bench.procedure" )
        public Stream<LongResult> procedure( @Name( "value" ) long value )
        {
            return LongStream.range( 0, value ).mapToObj( LongResult::new );
        }

        @Procedure( name = "bench.cypher" )
        public Stream<CypherResult> cypher( @Name( "cypher" ) String cypher )
        {
            return tx.execute( cypher ).stream().map( CypherResult::new );
        }
    }
}
