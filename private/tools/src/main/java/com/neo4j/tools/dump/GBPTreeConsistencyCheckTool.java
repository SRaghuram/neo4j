/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import java.io.File;
import java.util.List;
import java.util.StringJoiner;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBootstrapper;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyCheckVisitor;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.index.schema.SchemaLayouts;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class GBPTreeConsistencyCheckTool
{
    private static final String REPORT_DIRTY = "reportDirty";

    public static void main( String[] args ) throws Exception
    {
        Args arguments = Args.parse( args );
        if ( !validateArguments( arguments ) )
        {
            return;
        }
        File file = new File( arguments.orphans().get( 0 ) );
        final Boolean reportDirty = arguments.getBoolean( REPORT_DIRTY, true );
        new GBPTreeConsistencyCheckTool().run( file, reportDirty );
    }

    private void run( File file, boolean reportDirty ) throws Exception
    {
        System.out.println( "Check consistency on " + file.getAbsolutePath() );
        try ( DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
              JobScheduler jobScheduler = createInitialisedScheduler();
              GBPTreeBootstrapper bootstrapper = new GBPTreeBootstrapper( fs, jobScheduler, new SchemaLayouts(), true, NULL ) )
        {
            GBPTreeBootstrapper.Bootstrap bootstrap = bootstrapper.bootstrapTree( file );

            try ( GBPTree<?,?> tree = bootstrap.getTree() )
            {
                final GBPTreeConsistencyCheckVisitor visitor = loggingInconsistencyVisitor();
                System.out.println( "Starting consistency check" );
                boolean consistent = tree.consistencyCheck( visitor, reportDirty, PageCursorTracer.NULL );
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
        System.out.println( String.format( "Usage: [--reportDirty] gbpTreeFile%n" +
                "Must define file to consistency check.%n" +
                "--reportDirty%n" +
                "    Use this flag to control if dirty state and crashed pointers should be reported or not.%n" +
                "Supported layouts are:%n%s%n", layoutDescriptions.toString() ) );
    }
}

