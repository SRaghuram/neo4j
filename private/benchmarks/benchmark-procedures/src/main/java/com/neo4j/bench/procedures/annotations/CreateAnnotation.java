package com.neo4j.bench.procedures.annotations;

import com.neo4j.bench.client.model.Annotation;
import com.neo4j.bench.client.queries.AttachMetricsAnnotation;
import com.neo4j.bench.client.queries.AttachTestRunAnnotation;

import java.time.Instant;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class CreateAnnotation
{
    @Context
    public GraphDatabaseService db;

    @Procedure( name = "bench.createTestRunAnnotation", mode = Mode.WRITE )
    public Stream<CreateAnnotationRow> createAnnotation(
            @Name( "testRunId" ) String testRunId,
            @Name( "comment" ) String comment,
            @Name( "author" ) String author )
    {
        long now = Instant.now().toEpochMilli();
        Annotation annotation = new Annotation( comment, now, author );
        Node annotationNode = new AttachTestRunAnnotation( testRunId, annotation ).execute( db );
        return Stream.of( new CreateAnnotationRow( annotationNode ) );
    }

    @Procedure( name = "bench.createMetricsAnnotation", mode = Mode.WRITE )
    public Stream<CreateAnnotationRow> createAnnotation(
            @Name( "testRunId" ) String testRunId,
            @Name( "benchmarkName" ) String benchmarkName,
            @Name( "benchmarkGroupName" ) String benchmarkGroupName,
            @Name( "comment" ) String comment,
            @Name( "author" ) String author )
    {
        long now = Instant.now().toEpochMilli();
        Annotation annotation = new Annotation( comment, now, author );
        Node annotationNode =
                new AttachMetricsAnnotation( testRunId, benchmarkName, benchmarkGroupName, annotation ).execute( db );
        return Stream.of( new CreateAnnotationRow( annotationNode ) );
    }

    public class CreateAnnotationRow
    {
        public Node annotation;

        public CreateAnnotationRow( Node annotation )
        {
            this.annotation = annotation;
        }
    }
}
