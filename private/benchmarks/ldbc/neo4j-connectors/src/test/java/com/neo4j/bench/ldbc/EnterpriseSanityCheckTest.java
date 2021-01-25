/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@TestDirectoryExtension
public class EnterpriseSanityCheckTest
{
    @Inject
    public TestDirectory testFolder;

    @Test
    public void shouldUseInterpreted() throws Exception
    {
        shouldUseRuntime( Optional.of( "interpreted" ), "interpreted" );
    }

    @Test
    public void shouldUseSlotted() throws Exception
    {
        shouldUseRuntime( Optional.of( "slotted" ), "slotted" );
    }

    @Test
    public void shouldUsePipelined() throws Exception
    {
        shouldUseRuntime( Optional.of( "pipelined" ), "pipelined" );
    }

    @Test
    public void shouldDefaultToPipelined() throws Exception
    {
        shouldUseRuntime( Optional.empty(), "pipelined" );
    }

    private void shouldUseRuntime( Optional<String> maybeRequestedRuntime, String expectedRuntime ) throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        DatabaseManagementService managementService = Neo4jDb.newDb( dbDir, configFile() );
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        String requestedRuntime = maybeRequestedRuntime.isPresent() ? "runtime=" + maybeRequestedRuntime.get() : "";
        try ( Transaction transaction = db.beginTx() )
        {
            Result result = transaction.execute( "CYPHER " + requestedRuntime + " MATCH (n) RETURN n" );
            result.accept( row -> true );
            String planner = (String) result.getExecutionPlanDescription().getArguments().get( "planner" );
            String runtime = (String) result.getExecutionPlanDescription().getArguments().get( "runtime" );
            assertThat( planner.toLowerCase(), equalTo( "cost" ) );
            assertThat( runtime.toLowerCase(), equalTo( expectedRuntime ) );
            transaction.commit();
        }

        managementService.shutdown();
    }

    private File configFile() throws IOException
    {
        File neo4jConfigFile = testFolder.file( "neo4j.conf" ).toFile();
        Neo4jConfigBuilder.withDefaults()
                          .withSetting( record_format, "high_limit" )
                          .writeToFile( neo4jConfigFile.toPath() );
        return neo4jConfigFile;
    }
}
