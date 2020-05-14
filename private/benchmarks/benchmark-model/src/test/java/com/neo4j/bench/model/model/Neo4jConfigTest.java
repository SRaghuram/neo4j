/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;



@TestDirectoryExtension
public class Neo4jConfigTest
{
    @Inject
    public TestDirectory temporaryFolder;

    @Test
    void shouldSerialize()
    {
        assertSerialization( Neo4jConfig.empty() );
        assertSerialization( Neo4jConfig.empty().withSetting( "setting0", "value0" ) );
        assertSerialization( Neo4jConfig.empty().setJvmArgs( Collections.singletonList( "-Xmx1G" ) ) );
    }

    @Test
    void shouldBeImmutable()
    {
        // empty
        Neo4jConfig config0 = Neo4jConfig.empty();
        assertThat( config0.toMap().size(), equalTo( 0 ) );

        assertThat( config0.getJvmArgs(), equalTo( new ArrayList<>() ) );

        // add one setting
        Neo4jConfig config1 = config0.withSetting( "record_format", "high_limit" );
        assertThat( config0.toMap().size(), equalTo( 0 ) );
        assertThat( config1.toMap().size(), equalTo( 1 ) );

        assertThat( config1.toMap().get( "record_format" ), equalTo( "high_limit" ) );

        assertThat( config0.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config1.getJvmArgs(), equalTo( new ArrayList<>() ) );

        // overwrite setting
        Neo4jConfig config2 = config1.withSetting( "record_format", "standard" );
        assertThat( config0.toMap().size(), equalTo( 0 ) );
        assertThat( config1.toMap().size(), equalTo( 1 ) );
        assertThat( config2.toMap().size(), equalTo( 1 ) );

        assertThat( config1.toMap().get( "record_format" ), equalTo( "high_limit" ) );
        assertThat( config2.toMap().get( "record_format" ), equalTo( "standard" ) );

        assertThat( config0.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config1.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config2.getJvmArgs(), equalTo( new ArrayList<>() ) );

        // add one jvm arg
        Neo4jConfig config3 = config2.addJvmArg( "-Xms1g" );
        assertThat( config0.toMap().size(), equalTo( 0 ) );
        assertThat( config1.toMap().size(), equalTo( 1 ) );
        assertThat( config2.toMap().size(), equalTo( 1 ) );
        assertThat( config3.toMap().size(), equalTo( 1 ) );

        assertThat( config1.toMap().get( "record_format" ), equalTo( "high_limit" ) );
        assertThat( config2.toMap().get( "record_format" ), equalTo( "standard" ) );
        assertThat( config3.toMap().get( "record_format" ), equalTo( "standard" ) );

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
        assertThat( config1.toMap().get( "record_format" ), equalTo( "high_limit" ) );
        assertThat( config2.toMap().get( "record_format" ), equalTo( "standard" ) );
        assertThat( config3.toMap().get( "record_format" ), equalTo( "standard" ) );
        assertThat( config4.toMap().get( "record_format" ), equalTo( "standard" ) );

        assertThat( config0.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config1.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config2.getJvmArgs(), equalTo( new ArrayList<>() ) );
        assertThat( config3.getJvmArgs(), equalTo( Lists.newArrayList( "-Xms1g" ) ) );
        assertThat( config4.getJvmArgs(), equalTo( Lists.newArrayList( "-Xms2g", "-Xmx4g" ) ) );
    }

    @Test
    public void shouldMergeCorrectly()
    {
        assertThat( Neo4jConfig.empty().mergeWith( Neo4jConfig.empty() ), equalTo( Neo4jConfig.empty() ) );

        Neo4jConfig config0 = Neo4jConfig.empty()
                                         .setJvmArgs( Lists.newArrayList( "-Xms1g" ) )
                                         .addJvmArg( "-Xmx2g" )
                                         .withSetting( "setting1", "oldValue" );

        Neo4jConfig config1 = Neo4jConfig.empty()
                                         .setJvmArgs( Lists.newArrayList( "-Xmx2g" ) )
                                         .addJvmArg( "-XX:+UseG1GC" )
                                         .withSetting( "setting2", "false" )
                                         .withSetting( "setting1", "newValue" );

        Neo4jConfig config2 = config0.mergeWith( config1 );

        List<String> expectedJvmArgs = Lists.newArrayList( "-Xms1g", "-Xmx2g", "-XX:+UseG1GC" );

        Map<String,String> expectedSettings = new HashMap<>();
        expectedSettings.put( "setting1", "newValue" );
        expectedSettings.put( "setting2", "false" );

        assertThat( config2.toMap(), equalTo( expectedSettings ) );
        assertThat( config2.getJvmArgs(), equalTo( expectedJvmArgs ) );
    }

    @Test
    void shouldHandleJvmArgsCorrectly()
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

    @Test
    public void shouldOverwriteJvmArgs()
    {
        Neo4jConfig config0 = Neo4jConfig.empty().setJvmArgs( Lists.newArrayList( "-Xms1g" ) );

        Neo4jConfig config1 = config0.addJvmArg( "-Xms2g" );
        assertThat( config1.getJvmArgs(), contains( "-Xms2g" ) );
    }

    private void assertSerialization( Neo4jConfig config0 )
    {
        String json = config0.toJson();
        Neo4jConfig config1 = Neo4jConfig.fromJson( json );
        assertThat( config0, equalTo( config1 ) );
    }
}
