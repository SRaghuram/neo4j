/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.imports;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexPopulationProgress;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.Args;

public class CreateIndex
{
    private void run( String storeDir, List<String> indexPatterns )
    {
        GraphDatabaseService db = new CommercialGraphDatabaseFactory().newEmbeddedDatabase( new File( storeDir ) );
        try
        {
            Map<IndexDefinition,Integer> indexes = new HashMap<>();
            try ( Transaction tx = db.beginTx() )
            {
                for ( String indexPattern : indexPatterns )
                {
                    String[] split = splitPattern( indexPattern );
                    IndexDefinition index = db.schema().indexFor( Label.label( split[0] ) ).on( split[1] ).create();
                    indexes.put( index, 0 );
                }
                tx.success();
            }
            System.out.println( "Creating indexes:" );
            for ( IndexDefinition index : indexes.keySet() )
            {
                System.out.println( "  " + index );
            }
            try ( Transaction tx = db.beginTx() )
            {
                do
                {
                    try
                    {
                        db.schema().awaitIndexesOnline( 100, TimeUnit.MILLISECONDS );
                        break;
                    }
                    catch ( IllegalStateException e )
                    {
                        if ( !(e.getMessage().contains( "Expected index to come online within a reasonable time" ) ||
                                e.getMessage().contains( "Expected all indexes to come online within a reasonable time" )) )
                        {
                            throw e;
                        }
                        // else we just timed out and can try again
                    }
                    for ( IndexDefinition index : indexes.keySet() )
                    {
                        Integer prevComplete = indexes.get( index );
                        IndexPopulationProgress indexPopulationProgress = db.schema().getIndexPopulationProgress( index );
                        int currentComplete = (int) indexPopulationProgress.getCompletedPercentage() / 10;
                        if ( currentComplete > prevComplete )
                        {
                            reportProgress( index, prevComplete, currentComplete, "  " );
                            indexes.put( index, currentComplete );
                        }
                    }
                }
                while ( true );
                tx.success();
            }
            System.out.println( "Index creation finished:" );
            reportIndexStatus( db, indexes, "  " );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void reportIndexStatus( GraphDatabaseService db, Map<IndexDefinition,Integer> indexes, String indent )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : indexes.keySet() )
            {
                Schema.IndexState state = db.schema().getIndexState( index );
                System.out.println( String.format( "%s%s %s", indent, index.toString(), state ) );
            }
            tx.success();
        }
    }

    private void reportProgress( IndexDefinition index, int prevComplete, int currentComplete, String indent )
    {
        if ( prevComplete < currentComplete )
        {
            System.out.println( String.format( "%s%s %d%%", indent, index.toString(), currentComplete * 10 ) );
        }
    }

    private static String[] splitPattern( String indexPattern )
    {
        return indexPattern.split( ":" );
    }

    public static void main( String[] args )
    {
        Args argz = Args.parse( args );
        List<String> indexPatterns = argz.orphans();
        if ( !argz.has( "storeDir" ) || !indexPatternsOk( indexPatterns ) )
        {
            throw illegalArgsException( args );
        }
        new CreateIndex().run( argz.get( "storeDir" ), indexPatterns );
    }

    private static boolean indexPatternsOk( List<String> indexPatterns )
    {
        if ( indexPatterns.size() < 1 )
        {
            return false;
        }
        for ( String indexPattern : indexPatterns )
        {
            String[] split = splitPattern( indexPattern );
            if ( split.length != 2 )
            {
                return false;
            }
        }
        return true;
    }

    private static IllegalArgumentException illegalArgsException( String[] args )
    {
        return new IllegalArgumentException( "SYNTAX: --storeDir <dir> [<label>:<prop> ]+ provided arguments where " + Arrays.toString( args ) );
    }
}
