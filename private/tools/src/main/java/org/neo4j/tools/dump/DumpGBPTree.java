/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.TreePrinter;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;

import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

/**
 * For now only dumps header, could be made more useful over time.
 */
public class DumpGBPTree
{
    /**
     * Dumps stuff about a {@link GBPTree} to console in human readable format.
     *
     * @param args arguments.
     * @throws IOException on I/O error.
     */
    public static void main( String[] args ) throws IOException
    {
        if ( args.length == 0 )
        {
            System.err.println( "File argument expected" );
            System.exit( 1 );
        }

        File file = new File( args[0] );
        System.out.println( "Dumping " + file.getAbsolutePath() );
        TreePrinter.printHeader( new DefaultFileSystemAbstraction(), createInitialisedScheduler(), file, System.out );
    }
}
