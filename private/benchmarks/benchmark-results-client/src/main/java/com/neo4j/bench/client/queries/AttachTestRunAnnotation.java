/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries;

import com.neo4j.bench.common.model.Annotation;
import com.neo4j.bench.common.util.Resources;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static com.neo4j.bench.common.util.BenchmarkUtil.prettyPrint;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AttachTestRunAnnotation implements Query<Void>
{
    private static final String ATTACH_ANNOTATION = Resources.fileToString( "/queries/write/attach_test_run_annotation.cypher" );

    private String testRunId;
    private Annotation annotation;

    public AttachTestRunAnnotation( String testRunId, Annotation annotation )
    {
        this.testRunId = requireNonNull( testRunId );
        this.annotation = requireNonNull( annotation );
    }

    @Override
    public Void execute( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            StatementResult statementResult = session.run( ATTACH_ANNOTATION, params() );
            int annotationsCreated = statementResult.consume().counters().nodesCreated();
            if ( 1 != annotationsCreated )
            {
                throw new RuntimeException( format(
                        "Expected to create 1 annotation but created %s\n" +
                        "Failed to attach annotation %s to test run %s", annotationsCreated, annotation, testRunId ) );
            }
        }
        return null;
    }

    private Map<String,Object> params()
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "test_run_id", testRunId );
        params.put( "annotation", annotation.toMap() );
        return params;
    }

    @Override
    public String toString()
    {
        return "Params:\n" + prettyPrint( params() ) + ATTACH_ANNOTATION;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
