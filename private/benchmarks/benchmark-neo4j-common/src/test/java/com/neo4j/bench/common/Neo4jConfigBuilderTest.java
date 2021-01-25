/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common;

import com.google.common.collect.Lists;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestDirectoryExtension
public class Neo4jConfigBuilderTest
{

    @Inject
    public TestDirectory temporaryFolder;

    private final File defaultNeo4jConfigFile = FileUtils.toFile( Neo4jConfigBuilderTest.class.getResource( "/neo4j.conf" ) );

    @Test
    public void shouldSerialize()
    {
        assertSerialization( Neo4jConfigBuilder.withDefaults().build() );
        assertSerialization( Neo4jConfigBuilder.empty().setBoltUri( "http://localhost:7678" ).build() );
        assertSerialization( Neo4jConfigBuilder.empty().setDense( false ).setTransactionMemory( MemorySetting.DEFAULT.name() ).build() );
        assertSerialization( Neo4jConfigBuilder.empty().setTransactionMemory( MemorySetting.OFF_HEAP.name() ).build() );
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
        expectedSettings.put( "dbms.windows_service_name", "neo4j" );

        assertThat( config.getJvmArgs(), equalTo( expectedJvmArgs ) );
        assertThat( config.toMap(), equalTo( expectedSettings ) );
    }

    @Test
    public void shouldSerializeNeo4jConfigFromFile() throws IOException
    {
        // given
        File neo4jConfig = temporaryFolder.file( "config" ).toFile();
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
    public void shouldStoreSettingInFile()
    {
        Path neo4jConfigFile = temporaryFolder.file( "config" );
        Neo4jConfigBuilder.fromFile( defaultNeo4jConfigFile )
                          .withSetting( GraphDatabaseSettings.auth_enabled, "false" )
                          .writeToFile( neo4jConfigFile );

        Neo4jConfig neo4jConfig = Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build();
        assertEquals( "false", neo4jConfig.toMap().get( GraphDatabaseSettings.auth_enabled.name() ) );
    }

    @Test
    public void shouldReadJvmArgsFromFile()
    {

        Neo4jConfig neo4jConfig = Neo4jConfig.empty()
                                             .addJvmArg( "-Xmx1g" )
                                             .addJvmArg( "-Djava.io.tmpdir=/tmp/" )
                                             .addJvmArg( "-Xlog:gc,safepoint,gc+age=trace:file=gc.log:tags,time,uptime,level" );
        assertSerialization( neo4jConfig );
    }

    private Object serializeAndDeserialize( Object before )
    {
        Path jsonFile = temporaryFolder.file( "config" );
        JsonUtil.serializeJson( jsonFile, before );
        Object after = JsonUtil.deserializeJson( jsonFile, before.getClass() );
        assertThat( before, equalTo( after ) );
        return after;
    }

    private void assertSerialization( Neo4jConfig config0 )
    {
        Path configFile = temporaryFolder.file( "config" );
        Neo4jConfigBuilder.writeToFile( config0, configFile );
        Neo4jConfig config1 = Neo4jConfigBuilder.fromFile( configFile ).build();
        assertThat( config0, equalTo( config1 ) );
        String json = config1.toJson();
        Neo4jConfig config2 = Neo4jConfig.fromJson( json );
        assertThat( config1, equalTo( config2 ) );
    }
}
