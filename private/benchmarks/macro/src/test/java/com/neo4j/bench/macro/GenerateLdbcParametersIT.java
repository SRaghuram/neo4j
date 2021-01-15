/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Neo4jStore;
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
import java.io.UncheckedIOException;
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
import java.util.function.Consumer;

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

    private static class RowConsumer implements Consumer<Map<String,Object>>
    {
        private final String property;
        private final Set<String> ids;

        private RowConsumer( String property, Set<String> ids )
        {
            this.property = property;
            this.ids = ids;
        }

        @Override
        public void accept( Map<String,Object> row )
        {
            assert row.get( property ) != null;
            ids.add( String.valueOf( row.get( property ) ) );
        }
    }

    private void getParameters( String workloadName, Path storeDir ) throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.newFolder().toPath() ) )
        {
            Workload workload = Workload.fromName( workloadName, resources, Deployment.embedded() );
            try ( Store store = Neo4jStore.createFrom( storeDir ) )
            {
                try ( EmbeddedDatabase database = EmbeddedDatabase.startWith( store, Edition.ENTERPRISE, neo4jConfigFile() ) )
                {
                    GraphDatabaseService db = database.inner();

                    Set<String> messageIds = new HashSet<>();
                    Set<String> personIds = new HashSet<>();
                    Map<String,List<Consumer<Map<String,Object>>>> queryConsumers = new HashMap<>();

                    queryConsumers.put( "Read 1", Arrays.asList( new RowConsumer( "id", personIds ) ) );
                    queryConsumers.put( "Read 2", Arrays.asList( new RowConsumer( "personId", personIds ),
                                                                 new RowConsumer( "messageId", messageIds ) ) );
                    queryConsumers.put( "Read 3", Arrays.asList( new RowConsumer( "friendId", personIds ) ) );
                    queryConsumers.put( "Read 7", Arrays.asList( new RowConsumer( "personId", personIds ),
                                                                 new RowConsumer( "messageId", messageIds ) ) );
                    queryConsumers.put( "Read 8", Arrays.asList( new RowConsumer( "personId", personIds ),
                                                                 new RowConsumer( "commentId", messageIds ) ) );
                    queryConsumers.put( "Read 9", Arrays.asList( new RowConsumer( "personId", personIds ),
                                                                 new RowConsumer( "messageId", messageIds ) ) );
                    queryConsumers.put( "Read 10", Arrays.asList( new RowConsumer( "personId", personIds ) ) );
                    queryConsumers.put( "Read 11", Arrays.asList( new RowConsumer( "friendId", personIds ) ) );
                    queryConsumers.put( "Read 12", Arrays.asList( new RowConsumer( "friendId", personIds ) ) );

                    for ( Query query : workload.queries() )
                    {
                        List<Consumer<Map<String,Object>>> consumers = queryConsumers.getOrDefault( query.name(), null );
                        if ( consumers != null )
                        {
                            Parameters parameters = query.parameters();
                            ParametersReader parametersReader = parameters.create();

                            int count = 0;
                            while ( parametersReader.hasNext() && count < 1000 )
                            {
                                count++;
                                Map<String,Object> queryParameters = parametersReader.next();
                                try ( Transaction tx = db.beginTx() )
                                {
                                    try ( Result result = tx.execute( query.queryString().value(), queryParameters ) )
                                    {
                                        result.forEachRemaining( row -> consumers.forEach( consumer -> consumer.accept( row ) ) );
                                    }
                                }
                            }
                        }
                    }
                    writeToFile( "Message:Long", messageIds, workloadName );
                    writeToFile( "Person:Long", personIds, workloadName );
                }
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
                                  throw new UncheckedIOException( e );
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
