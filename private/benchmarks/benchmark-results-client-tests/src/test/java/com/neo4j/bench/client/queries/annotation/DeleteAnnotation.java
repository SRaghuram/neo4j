/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.annotation;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Annotation;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.SummaryCounters;

import static com.neo4j.bench.model.util.MapPrinter.prettyPrint;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DeleteAnnotation implements Query<Void>
{
    private static final String DELETE_ANNOTATION = Resources.fileToString( "/queries/annotations/delete_annotation.cypher" );

    private Annotation annotation;

    public DeleteAnnotation( Annotation annotation )
    {
        this.annotation = requireNonNull( annotation );
    }

    @Override
    public Void execute( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Result statementResult = session.run( DELETE_ANNOTATION, params() );
            SummaryCounters counters = statementResult.consume().counters();
            assertDeleted( counters.nodesDeleted(), counters.relationshipsDeleted() );
        }
        return null;
    }

    private void assertDeleted( int nodesDeleted, int relationshipsDeleted )
    {
        if ( 1 != nodesDeleted || 1 != relationshipsDeleted )
        {
            throw new RuntimeException( format(
                    "Failed to delete annotation: %s\n" +
                    "Expected to delete one node & one relationship\n" +
                    "Actually deleted %s nodes & %s relationships",
                    annotation, nodesDeleted, relationshipsDeleted ) );
        }
    }

    private Map<String,Object> params()
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "event_id", annotation.eventId() );
        return params;
    }

    @Override
    public String toString()
    {
        return "Params:\n" + prettyPrint( params() ) + DELETE_ANNOTATION;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
