/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dbstructure;

import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.impl.util.dbstructure.DbStructureArgumentFormatter;
import org.neo4j.kernel.impl.util.dbstructure.DbStructureVisitor;
import org.neo4j.kernel.impl.util.dbstructure.GraphDbStructureGuide;
import org.neo4j.kernel.impl.util.dbstructure.InvocationTracer;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class DbStructureTool
{
    protected DbStructureTool()
    {
    }

    public static void main( String[] args ) throws IOException
    {
        new DbStructureTool().run( args );
    }

    protected void run( String[] args ) throws IOException
    {
        if ( args.length != 2 && args.length != 3 )
        {
            System.err.println( "arguments: <generated class name> [<output source root>] <database dir>" );
            System.exit( 1 );
        }

        boolean writeToFile = args.length == 3;
        String generatedClassWithPackage = args[0];
        String dbDir = writeToFile ? args[2] : args[1];

        Pair<String, String> parsedGenerated = parseClassNameWithPackage( generatedClassWithPackage );
        String generatedClassPackage = parsedGenerated.first();
        String generatedClassName = parsedGenerated.other();

        String generator = format( "%s %s [<output source root>] <db-dir>",
                getClass().getCanonicalName(),
                generatedClassWithPackage
        );

        DatabaseManagementService managementService = instantiateGraphDatabase( dbDir );
        GraphDatabaseService graph = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            if ( writeToFile )
            {
                Path sourceRoot = Path.of( args[1] );
                String outputPackageDir = generatedClassPackage.replace( '.', sourceRoot.getFileSystem().getSeparator().charAt( 0 ) );
                String outputFileName = generatedClassName + ".java";
                Path outputDir = sourceRoot.resolve( outputPackageDir );
                Path outputFile = outputDir.resolve( outputFileName );
                try ( PrintWriter writer = new PrintWriter( Files.newOutputStream( outputFile ) ) )
                {
                    traceDb( generator, generatedClassPackage, generatedClassName, graph, writer );
                }
            }
            else
            {
                traceDb( generator, generatedClassPackage, generatedClassName, graph, System.out );
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static DatabaseManagementService instantiateGraphDatabase( String dbDir )
    {
        return new EnterpriseDatabaseManagementServiceBuilder( Path.of( dbDir ) ).build();
    }

    private static void traceDb( String generator, String generatedClazzPackage, String generatedClazzName, GraphDatabaseService graph, Appendable output )
            throws IOException
    {
        InvocationTracer<DbStructureVisitor> tracer = new InvocationTracer<>(
                generator,
                generatedClazzPackage,
                generatedClazzName,
                DbStructureVisitor.class,
                DbStructureArgumentFormatter.INSTANCE,
                output
        );

        DbStructureVisitor visitor = tracer.newProxy();
        GraphDbStructureGuide guide = new GraphDbStructureGuide( graph );
        guide.accept( visitor );
        tracer.close();
    }

    private static Pair<String, String> parseClassNameWithPackage( String classNameWithPackage )
    {
        if ( classNameWithPackage.contains( "%" ) )
        {
            throw new IllegalArgumentException(
                "Format character in generated class name: " + classNameWithPackage
            );
        }

        int index = classNameWithPackage.lastIndexOf( '.' );

        if ( index < 0 )
        {
            throw new IllegalArgumentException(
                "Expected fully qualified class name but got: " + classNameWithPackage
            );
        }

        return Pair.of(
            classNameWithPackage.substring( 0, index ),
            classNameWithPackage.substring( index + 1 )
        );
    }
}
