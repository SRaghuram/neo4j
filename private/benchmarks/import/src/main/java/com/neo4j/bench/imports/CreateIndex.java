/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.imports;

import com.neo4j.bench.client.database.Store;
import com.neo4j.commercial.edition.factory.CommercialDatabaseManagementServiceBuilder;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexPopulationProgress;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.Args;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class CreateIndex
{
    private void run( String storeDirString, List<String> indexPatterns )
    {
        Store store = Store.createFrom( Paths.get( storeDirString ) );
        DatabaseManagementService managementService = new CommercialDatabaseManagementServiceBuilder()
                .newEmbeddedDatabaseBuilder( store.topLevelDirectory().toFile() )
                .newDatabaseManagementService();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            Map<IndexDefinition,Integer> indexes = new HashMap<>();
            try ( Transaction tx = db.beginTx() )
            {
                for ( String indexPattern : indexPatterns )
                {
                    String[] labelAndProperties = splitLabel( indexPattern );
                    String[] properties = splitProperties( labelAndProperties[1] );
                    IndexCreator indexCreator = db.schema().indexFor( Label.label( labelAndProperties[0] ) );
                    for ( String property : properties )
                    {
                        indexCreator = indexCreator.on( property );
                    }
                    IndexDefinition index = indexCreator.create();
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
            managementService.shutdown();
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

    private static String[] splitLabel( String indexPattern )
    {
        return indexPattern.split( ":" );
    }

    private static String[] splitProperties( String properties )
    {
        return properties.split( "," );
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
            String[] split = splitLabel( indexPattern );
            if ( split.length != 2 )
            {
                return false;
            }
            if ( splitProperties( split[1] ).length < 1 )
            {
                return false;
            }
        }
        return true;
    }

    private static IllegalArgumentException illegalArgsException( String[] args )
    {
        return new IllegalArgumentException( "SYNTAX: --storeDir <dir> [<label>:<prop1[,prop2]+> ]+ provided arguments where " + Arrays.toString( args ) );
    }
}
