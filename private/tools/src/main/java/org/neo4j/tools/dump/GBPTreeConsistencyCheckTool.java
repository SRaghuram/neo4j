/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump;

import java.io.File;
import java.util.List;
import java.util.StringJoiner;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.helpers.Args;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBootstrapper;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyCheckVisitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.annotations.ReporterFactory;
import org.neo4j.kernel.impl.index.schema.SchemaLayouts;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class GBPTreeConsistencyCheckTool
{
    private static final String LAYOUT = "layout";
    private static final String ALLOW_CRASH = "allowCrash";

    /**
     * Usage: --layout "layout" --allowCrash gbpTreeFile
     *
     * --layout "layout"
     * What layout is the expected layout of the gbpTree?
     *
     * --allowCrash
     * Set this flag to allow crash pointers in tree.
     */
    public static void main( String[] args ) throws Exception
    {
        Args arguments = Args.withFlags( ALLOW_CRASH ).parse( args );
        if ( !validateArguments( arguments ) )
        {
            return;
        }
        File file = new File( arguments.orphans().get( 0 ) );
        String layoutIdentifier = arguments.get( LAYOUT );
        new GBPTreeConsistencyCheckTool().run( file, layoutIdentifier, arguments.getBoolean( ALLOW_CRASH, false ) );
    }

    private void run( File file, String layoutIdentifier, boolean allowCrashPointers ) throws Exception
    {
        System.out.println( "Check consistency on " + file.getAbsolutePath() );
        try ( JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pageCache = GBPTreeBootstrapper.pageCache( jobScheduler ) )
        {
            final GBPTreeBootstrapper bootstrapper = new GBPTreeBootstrapper( pageCache, new SchemaLayouts(), true );
            final GBPTreeBootstrapper.Bootstrap bootstrap = bootstrapper.bootstrapTree( file, layoutIdentifier );

            try ( GBPTree<?,?> tree = bootstrap.getTree() )
            {
                final GBPTreeConsistencyCheckVisitor visitor = loggingInconsistencyVisitor();
                System.out.println( "Starting consistency check" );
                boolean consistent = tree.consistencyCheck( visitor, allowCrashPointers );
                if ( consistent )
                {
                    System.out.println( "Consistency check finished successful." );
                }
                else
                {
                    System.out.println( "Consistency check finished with inconsistencies." );
                }
            }
        }
    }

    private GBPTreeConsistencyCheckVisitor loggingInconsistencyVisitor()
    {
        final FormattedLogProvider logProvider = FormattedLogProvider.toOutputStream( System.out );
        final FormattedLog log = logProvider.getLog( GBPTreeConsistencyCheckTool.class );
        InconsistencyReport report = new InconsistencyReport( new InconsistencyMessageLogger( log ), new ConsistencySummaryStatistics() );
        ConsistencyReporter.FormattingDocumentedHandler handler = ConsistencyReporter.formattingHandler( report, RecordType.INDEX );
        return new ReporterFactory( handler ).getClass( GBPTreeConsistencyCheckVisitor.class );
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
        System.out.println( String.format( "Usage: [--allowCrash] [--layout <identifier>] gbpTreeFile%n" +
                "Must define file to consistency check.%n" +
                "--layout <identifier>%n" +
                "    What is the expected layout of the gbpTree? In many cases the%n" +
                "    bootstrapper can figure this out without this setting but in%n" +
                "    some cases it needs help from user. Here are all supported%n" +
                "    layouts and the corresponding identifier for each of them:%n%s%n" +
                "--allowCrash%n" +
                "    Set this flag to allow crash pointers in tree.", layoutDescriptions.toString() ) );
    }
}

