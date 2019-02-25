/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.util.MapUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

public class EnterpriseSanityCheckTest
{
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

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
    public void shouldUseCompiled() throws Exception
    {
        shouldUseRuntime( Optional.of( "compiled" ), "compiled" );
    }

    @Test
    public void shouldDefaultToCompiled() throws Exception
    {
        shouldUseRuntime( Optional.empty(), "compiled" );
    }

    private void shouldUseRuntime( Optional<String> maybeRequestedRuntime, String expectedRuntime ) throws Exception
    {
        File dbDir = testFolder.newFolder();
        GraphDatabaseService db = Neo4jDb.newDb( dbDir, configFile() );
        String requestedRuntime = maybeRequestedRuntime.isPresent() ? "runtime=" + maybeRequestedRuntime.get() : "";
        Result result = db.execute( "CYPHER " + requestedRuntime + " MATCH (n) RETURN n" );
        result.accept( row -> true );
        String planner = (String) result.getExecutionPlanDescription().getArguments().get( "planner" );
        String runtime = (String) result.getExecutionPlanDescription().getArguments().get( "runtime" );
        assertThat( planner.toLowerCase(), equalTo( "cost" ) );
        assertThat( runtime.toLowerCase(), equalTo( expectedRuntime ) );
        db.shutdown();
    }

    private File configFile() throws IOException
    {
        File neo4jConfigFile = testFolder.newFile();
        Map<String,String> neo4jConfigMap = new HashMap<>();
        neo4jConfigMap.put( record_format.name(), "high_limit" );
        Properties neo4jConfigProperties = MapUtils.mapToProperties( neo4jConfigMap );
        neo4jConfigProperties.store( new FileOutputStream( neo4jConfigFile ), null );
        return neo4jConfigFile;
    }

}
