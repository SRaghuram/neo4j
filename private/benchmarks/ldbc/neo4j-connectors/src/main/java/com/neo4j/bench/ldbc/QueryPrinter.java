/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveCypherQueries;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import static java.lang.String.format;

public class QueryPrinter
{
    private static final Logger LOG = LoggerFactory.getLogger( QueryPrinter.class );

    public static void main( String[] args ) throws IOException, DbException
    {
        if ( args.length != 1 )
        {
            throw new DbException(
                    format( "Expected 1 arg (directory), found %s: %s", args.length, Arrays.toString( args ) )
            );
        }
        File dir = new File( args[0] );
        if ( !dir.exists() )
        {
            throw new DbException( format( "Directory does not exist: %s", dir.getAbsolutePath() ) );
        }
        writeQueriesToFile( dir );
    }

    private static void writeQueriesToFile( File dir ) throws IOException, DbException
    {
        SnbInteractiveCypherQueries queries = SnbInteractiveCypherQueries.createWith(
                Planner.DEFAULT,
                Runtime.DEFAULT );
        File file = new File( dir, "queries.txt" );
        if ( file.exists() )
        {
            FileUtils.forceDelete( file );
            file.createNewFile();
        }
        LOG.debug( "Writing query strings to: " + file.getAbsolutePath() );
        BufferedWriter writer = new BufferedWriter( new FileWriter( file ) );
        writer.write( "QUERY 1" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read1() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 2" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read2() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 3" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read3() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 4" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read4() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 5" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read5() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 6" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read6() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 7" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read7() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 8" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read8() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 9" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read9() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 10" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read10() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 11" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read11() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 12" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read12() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 13" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read13() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "QUERY 14" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.read14() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "UPDATE 1" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.write1() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "UPDATE 2" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.write2() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "UPDATE 3" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.write3() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "UPDATE 4" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.write4() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "UPDATE 5" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.write5() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "UPDATE 6" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.write6() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "UPDATE 7" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.write7() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "UPDATE 8" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.write8() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "SHORT READ 1" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.short1() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "SHORT READ 2" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.short2() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "SHORT READ 3" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.short3() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "SHORT READ 4" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.short4() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "SHORT READ 5" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.short5() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "SHORT READ 6" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.short6() ).queryString() );
        writer.newLine();
        writer.newLine();
        writer.write( "SHORT READ 7" );
        writer.newLine();
        writer.newLine();
        writer.write( queries.queryFor( DummyLdbcSnbInteractiveOperationInstances.short7() ).queryString() );
        writer.flush();
        writer.close();
    }
}
