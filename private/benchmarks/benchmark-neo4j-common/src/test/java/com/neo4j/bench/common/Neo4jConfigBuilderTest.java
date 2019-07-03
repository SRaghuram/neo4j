/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.util.JsonUtil;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import static java.util.Arrays.asList;

public class Neo4jConfigBuilderTest
{

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File defaultNeo4jConfigFile = FileUtils.toFile( Neo4jConfigBuilderTest.class.getResource( "/neo4j.conf" ) );

    @Test
    public void shouldSerialize() throws IOException
    {
        assertSerialization( Neo4jConfigBuilder.withDefaults().build() );
        assertSerialization( Neo4jConfigBuilder.empty().setBoltUri( "http://localhost:7678" ).build() );
        assertSerialization( Neo4jConfigBuilder.empty().setDense( false ).setTransactionMemory( "native" ).build() );
        assertSerialization( Neo4jConfigBuilder.empty().addJvmArgs( asList( "-Xmx4g", "-Xms4g" ) ).build() );
        assertSerialization( Neo4jConfigBuilder.fromFile( defaultNeo4jConfigFile ).build() );
    }

    @Test
    public void shouldReadFromFile()
    {
        Neo4jConfig config = Neo4jConfigBuilder.fromFile( defaultNeo4jConfigFile ).build();

        List<String> expectedJvmArgs = Lists.newArrayList( "-XX:+UseG1GC",
                                                           "-XX:-OmitStackTraceInFastThrow",
                                                           "-XX:+AlwaysPreTouch",
                                                           "-XX:+UnlockExperimentalVMOptions",
                                                           "-XX:+TrustFinalNonStaticFields",
                                                           "-XX:+DisableExplicitGC",
                                                           "-Djdk.tls.ephemeralDHKeySize=2048",
                                                           "-Djdk.tls.rejectClientInitiatedRenegotiation=true",
                                                           "-Dunsupported.dbms.udc.source=tarball" );

        Map<String,String> expectedSettings = new HashMap<>();
        expectedSettings.put( "dbms.directories.import", "import" );
        expectedSettings.put( "dbms.connector.bolt.enabled", "true" );
        expectedSettings.put( "dbms.connector.http.enabled", "true" );
        expectedSettings.put( "dbms.connector.https.enabled", "true" );
        expectedSettings.put( "ha.pull_interval", "10" );
        expectedSettings.put( "dbms.windows_service_name", "neo4j" );

        assertThat( config.getJvmArgs(), equalTo( expectedJvmArgs ) );
        assertThat( config.toMap(), equalTo( expectedSettings ) );
    }

    @Test
    public void shouldSerializeNeo4jConfigFromFile() throws IOException
    {
        // given
        File neo4jConfig = temporaryFolder.newFile();
        FileWriter fileWriter = new FileWriter( neo4jConfig );
        fileWriter.append( "key1=value1" );
        fileWriter.append( "\n" );
        fileWriter.append( "key2=value2" );
        fileWriter.flush();
        fileWriter.close();

        Neo4jConfig before = Neo4jConfigBuilder.fromFile( neo4jConfig ).build();
        // then
        Neo4jConfig after = (Neo4jConfig) serializeAndDeserialize( before );
        assertThat( before.toMap().get( "key1" ), equalTo( after.toMap().get( "key1" ) ) );
        assertThat( before.toMap().get( "key2" ), equalTo( after.toMap().get( "key2" ) ) );
    }

    @Test
    public void shouldStoreSettingInFile() throws Exception
    {
        Path neo4jConfigFile = temporaryFolder.newFile().toPath();
        Neo4jConfigBuilder.fromFile( defaultNeo4jConfigFile )
        .withSetting( GraphDatabaseSettings.auth_enabled, "false" )
        .writeToFile( neo4jConfigFile );

        Neo4jConfig neo4jConfig = Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build();
        assertEquals( "false", neo4jConfig.toMap().get( GraphDatabaseSettings.auth_enabled.name() ) );
    }

    private Object serializeAndDeserialize( Object before ) throws IOException
    {
        File jsonFile = temporaryFolder.newFile();
        JsonUtil.serializeJson( jsonFile.toPath(), before );
        Object after = JsonUtil.deserializeJson( jsonFile.toPath(), before.getClass() );
        assertThat( before, equalTo( after ) );
        return after;
    }

    private void assertSerialization( Neo4jConfig config0 ) throws IOException
    {
        Path configFile = temporaryFolder.newFile().toPath();
        Neo4jConfigBuilder.writeToFile( config0, configFile );
        Neo4jConfig config1 = Neo4jConfigBuilder.fromFile( configFile ).build();
        assertThat( config0, equalTo( config1 ) );
        String json = config1.toJson();
        Neo4jConfig config2 = Neo4jConfig.fromJson( json );
        assertThat( config1, equalTo( config2 ) );
    }
}
