/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.workload.Parameters;
import com.neo4j.bench.macro.workload.ParametersReader;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.options.Edition;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class GenerateLdbcParametersIT
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Ignore
    @Test
    public void findLDBC01() throws Exception
    {
        Path storeDir = Paths.get( "ldbc01" );
        String workloadName = "ldbc_sf001";
        getParameters( workloadName, storeDir );
    }

    @Ignore
    @Test
    public void findLDBC010() throws Exception
    {
        Path storeDir = Paths.get( "ldbc10" );
        String workloadName = "ldbc_sf010";
        getParameters( workloadName, storeDir );
    }

    private void getParameters( String workloadName, Path storeDir ) throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.newFolder().toPath() ) )
        {
            Workload workload = Workload.fromName( workloadName, resources, Deployment.embedded() );
            try ( Store store = Store.createFrom( storeDir ) )
            {
                EmbeddedDatabase database = EmbeddedDatabase.startWith( store, Edition.ENTERPRISE, neo4jConfigFile() );
                GraphDatabaseService db = database.db();

                Map<Query,List<String>> queries = new HashMap<>();

                queries.put( workload.queryForName( "Read 1" ),
                             Arrays.asList( "id" ) );
                queries.put( workload.queryForName( "Read 2" ),
                             Arrays.asList( "personId", "messageId" ) );
                queries.put( workload.queryForName( "Read 3" ),
                             Arrays.asList( "friendId" ) );
                queries.put( workload.queryForName( "Read 7" ),
                             Arrays.asList( "personId", "messageId" ) );
                queries.put( workload.queryForName( "Read 8" ),
                             Arrays.asList( "personId", "commentId" ) );
                queries.put( workload.queryForName( "Read 9" ),
                             Arrays.asList( "personId", "messageId" ) );
                queries.put( workload.queryForName( "Read 10" ),
                             Arrays.asList( "friendId" ) );
                queries.put( workload.queryForName( "Read 11" ),
                             Arrays.asList( "personId" ) );
                queries.put( workload.queryForName( "Read 12" ),
                             Arrays.asList( "friendId" ) );
                queries.put( workload.queryForName( "Read 14" ),
                             Arrays.asList( "personId" ) );
                Set<String> messageIds = new HashSet();
                Set<String> personIds = new HashSet();

                for ( Query query : queries.keySet() )
                {
                    List<String> queryKeys = queries.get( query );
                    Parameters parameters = query.parameters();
                    ParametersReader parametersReader = parameters.create();

                    //all of these parameters are loop able, so we need to cancel after a while.
                    //and 1000 iterations should be more then enough
                    int count = 0;
                    while ( parametersReader.hasNext() && count < 1000 )
                    {
                        count++;
                        Map<String,Object> queryParameters = parametersReader.next();
                        try ( Transaction tx = db.beginTx() )
                        {
                            try ( Result result = db.execute( query.queryString().value(), queryParameters ) )
                            {
                                result.stream().forEach( row ->
                                                                 queryKeys.forEach( key ->
                                                                                    {
                                                                                        if ( key.equals( "id" ) || key.equals( "personId" ) ||
                                                                                             key.equals( "friendId" ) )
                                                                                        {
                                                                                            personIds.add( String.valueOf( row.get( key ) ) );
                                                                                        }
                                                                                        else
                                                                                        {
                                                                                            messageIds.add( String.valueOf( row.get( key ) ) );
                                                                                        }
                                                                                    } )
                                );
                                tx.success();
                            }
                        }
                    }
                }
                writeToFile( "Message:Long", messageIds, workloadName );
                writeToFile( "Person:Long", personIds,workloadName );
            }
        }
    }

    private void writeToFile( String name, Set<String> ids, String workloadName ) throws IOException
    {
        FileWriter fileWriter = new FileWriter( workloadName + name + ".txt" );
        try ( BufferedWriter bufferedWriter = new BufferedWriter( fileWriter ) )
        {
            bufferedWriter.write( name );
            List<String> idList = new ArrayList<>( ids );
            Collections.shuffle( idList );
            idList.forEach( row ->
                          {
                              try
                              {
                                  bufferedWriter.newLine();
                                  bufferedWriter.write( row );
                              }
                              catch ( IOException e )
                              {
                                  throw new RuntimeException( e );
                              }
                          } );
            bufferedWriter.flush();
        }
    }

    private Path neo4jConfigFile() throws Exception
    {
        Path neo4jConfigFile = temporaryFolder.newFile().toPath();
        Neo4jConfig neo4jConfig = Neo4jConfigBuilder.empty().build();
        Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );
        return neo4jConfigFile;
    }
}
