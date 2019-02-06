/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.annotations;

import com.neo4j.bench.client.model.Annotation;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class DeleteAnnotation
{
    @Context
    public GraphDatabaseService db;

    @Procedure( name = "bench.deleteAnnotation", mode = Mode.WRITE )
    public void deleteAnnotation(
            @Name( "Date" ) Long date,
            @Name( "comment" ) String comment,
            @Name( "author" ) String author,
            @Name( "eventId" ) String eventId )
    {
        Annotation annotation = new Annotation( comment, date, eventId, author );
        new com.neo4j.bench.client.queries.DeleteAnnotation( annotation ).execute( db );
    }
}
