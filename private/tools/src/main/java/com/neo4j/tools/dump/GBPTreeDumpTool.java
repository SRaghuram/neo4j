/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.StringJoiner;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBootstrapper;
import org.neo4j.index.internal.gbptree.PrintingGBPTreeVisitor;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.SchemaLayouts;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class GBPTreeDumpTool
{
    /**
     * Usage: gbpTreeFile
     */
    public static void main( String[] args ) throws Exception
    {
        Args arguments = Args.parse( args );
        if ( !validateArguments( arguments ) )
        {
            return;
        }
        File file = new File( arguments.orphans().get( 0 ) );
        new GBPTreeDumpTool().run( file, System.out );
    }

    void run( File file, PrintStream out ) throws Exception
    {
        out.println( "Dump tree " + file.getAbsolutePath() );
        try ( JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pageCache = GBPTreeBootstrapper.pageCache( jobScheduler ) )
        {
            final GBPTreeBootstrapper bootstrapper = new GBPTreeBootstrapper( pageCache, new SchemaLayouts(), true );
            final GBPTreeBootstrapper.Bootstrap bootstrap = bootstrapper.bootstrapTree( file );

            try ( GBPTree<?,?> tree = bootstrap.getTree() )
            {
                tree.visit( new PrintingGBPTreeVisitor<>( out, false, false, false, true, false, false ) );
            }
        }
    }

    private static boolean validateArguments( Args arguments )
    {
        List<String> gbpTreeFile = arguments.orphans();
        if ( gbpTreeFile.size() != 1 )
        {
            printUsage();
            return false;
        }
        return true;
    }

    private static void printUsage()
    {
        StringJoiner layoutDescriptions = new StringJoiner( String.format( "%n    * " ), "    * ", "" ).setEmptyValue( "No layouts available" );
        for ( String layoutDescription : SchemaLayouts.layoutDescriptions() )
        {
            layoutDescriptions.add( layoutDescription );
        }
        System.out.println( String.format( "Usage: gbpTreeFile%n" +
                "Must define file to dump.%n" +
                "Supported layouts are:%n%s%n", layoutDescriptions.toString() ) );
    }
}

