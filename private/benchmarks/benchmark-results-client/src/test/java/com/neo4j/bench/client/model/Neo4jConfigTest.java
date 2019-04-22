/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.ext.udc.UdcSettings.udc_enabled;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

public class Neo4jConfigTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File defaultNeo4jConfigFile = FileUtils.toFile( Neo4jConfigTest.class.getResource( "/neo4j.conf" ) );

    @Test
    public void shouldSerialize() throws IOException
    {
        assertSerialization( Neo4jConfig.empty() );
        assertSerialization( Neo4jConfig.withDefaults() );
        assertSerialization( Neo4jConfig.fromFile( defaultNeo4jConfigFile ) );
    }

    @Test
    public void shouldBeImmutable()
    {
        // empty
        Neo4jConfig config0 = Neo4jConfig.empty();
        assertThat( config0.toMap().size(), equalTo( 0 ) );

        assertThat( config0.getJvmArgs(), equalTo( new ArrayList<>() ) );

        // add one setting
        Neo4jConfig config1 = config0.withSetting( record_format, "high_limit" );
        assertThat( config0.toMap().size(), equalTo( 0 ) );
        assertThat( config1.toMap().size(), equalTo( 1 ) );

        assertThat( config1.toMap().get( record_format.name() ), equalTo( "high_limit" ) );

        assertThat( config0.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config1.getJvmArgs(), equalTo( new ArrayList<>() ) );

        // overwrite setting
        Neo4jConfig config2 = config1.withSetting( record_format, "standard" );
        assertThat( config0.toMap().size(), equalTo( 0 ) );
        assertThat( config1.toMap().size(), equalTo( 1 ) );
        assertThat( config2.toMap().size(), equalTo( 1 ) );

        assertThat( config1.toMap().get( record_format.name() ), equalTo( "high_limit" ) );
        assertThat( config2.toMap().get( record_format.name() ), equalTo( "standard" ) );

        assertThat( config0.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config1.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config2.getJvmArgs(), equalTo( new ArrayList<>() ) );

        // add one jvm arg
        Neo4jConfig config3 = config2.addJvmArg( "-Xms1g" );
        assertThat( config0.toMap().size(), equalTo( 0 ) );
        assertThat( config1.toMap().size(), equalTo( 1 ) );
        assertThat( config2.toMap().size(), equalTo( 1 ) );
        assertThat( config3.toMap().size(), equalTo( 1 ) );

        assertThat( config1.toMap().get( record_format.name() ), equalTo( "high_limit" ) );
        assertThat( config2.toMap().get( record_format.name() ), equalTo( "standard" ) );
        assertThat( config3.toMap().get( record_format.name() ), equalTo( "standard" ) );

        assertThat( config0.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config1.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config2.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config3.getJvmArgs(), equalTo( Lists.newArrayList( "-Xms1g" ) ) );

        // overwrite jvm arg
        Neo4jConfig config4 = config3.setJvmArgs( Lists.newArrayList( "-Xms2g", "-Xmx4g" ) );
        assertThat( config0.toMap().size(), equalTo( 0 ) );
        assertThat( config1.toMap().size(), equalTo( 1 ) );
        assertThat( config2.toMap().size(), equalTo( 1 ) );
        assertThat( config3.toMap().size(), equalTo( 1 ) );
        assertThat( config4.toMap().size(), equalTo( 1 ) );
        assertThat( config1.toMap().get( record_format.name() ), equalTo( "high_limit" ) );
        assertThat( config2.toMap().get( record_format.name() ), equalTo( "standard" ) );
        assertThat( config3.toMap().get( record_format.name() ), equalTo( "standard" ) );
        assertThat( config4.toMap().get( record_format.name() ), equalTo( "standard" ) );

        assertThat( config0.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config1.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config2.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config3.getJvmArgs(), equalTo( Lists.newArrayList( "-Xms1g" ) ) );
        assertThat( config4.getJvmArgs(), equalTo( Lists.newArrayList( "-Xms2g", "-Xmx4g" ) ) );
    }

    @Test
    public void shouldReadFromFile()
    {
        Neo4jConfig config = Neo4jConfig.fromFile( defaultNeo4jConfigFile );

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
    public void shouldMergeCorrectly()
    {
        assertThat( Neo4jConfig.empty().mergeWith( Neo4jConfig.empty() ), equalTo( Neo4jConfig.empty() ) );

        Neo4jConfig config0 = Neo4jConfig.empty()
                                         .setJvmArgs( Lists.newArrayList( "-Xms1g" ) )
                                         .addJvmArg( "-Xmx2g" )
                                         .withSetting( record_format, "high_limit" );

        Neo4jConfig config1 = Neo4jConfig.empty()
                                         .setJvmArgs( Lists.newArrayList( "-Xmx2g" ) )
                                         .addJvmArg( "-XX:+UseG1GC" )
                                         .withSetting( udc_enabled, "false" )
                                         .withSetting( record_format, "standard" );

        Neo4jConfig config2 = config0.mergeWith( config1 );

        List<String> expectedJvmArgs = Lists.newArrayList( "-Xms1g", "-Xmx2g", "-XX:+UseG1GC" );

        Map<String,String> expectedSettings = new HashMap<>();
        expectedSettings.put( udc_enabled.name(), "false" );
        expectedSettings.put( record_format.name(), "standard" );

        assertThat( config2.toMap(), equalTo( expectedSettings ) );
        assertThat( config2.getJvmArgs(), equalTo( expectedJvmArgs ) );
    }

    @Test
    public void shouldHandleJvmArgsCorrectly()
    {
        Neo4jConfig config0 = Neo4jConfig.empty().setJvmArgs( Lists.newArrayList( "-Xms1g" ) );
        List<String> expectedJvmArgs0 = Lists.newArrayList( "-Xms1g" );
        assertThat( config0.getJvmArgs(), equalTo( expectedJvmArgs0 ) );

        Neo4jConfig config1 = config0.addJvmArg( "-Xmx1g" );
        List<String> expectedJvmArgs1 = new ArrayList<>( expectedJvmArgs0 );
        expectedJvmArgs1.add( "-Xmx1g" );
        assertThat( config1.getJvmArgs(), equalTo( expectedJvmArgs1 ) );
        assertThat( config1.toMap(), equalTo( config0.toMap() ) );

        Neo4jConfig config2 = config1.setJvmArgs( Lists.newArrayList( "-Xmx1g" ) );
        assertThat( config2.getJvmArgs(), equalTo( Lists.newArrayList( "-Xmx1g" ) ) );
        assertThat( config2.toMap(), equalTo( config1.toMap() ) );
    }

    private void assertSerialization( Neo4jConfig config0 ) throws IOException
    {
        Path configFile = temporaryFolder.newFile().toPath();
        config0.writeToFile( configFile );
        Neo4jConfig config1 = Neo4jConfig.fromFile( configFile );
        assertThat( config0, equalTo( config1 ) );
        String json = config1.toJson();
        Neo4jConfig config2 = Neo4jConfig.fromJson( json );
        assertThat( config1, equalTo( config2 ) );
    }
}
