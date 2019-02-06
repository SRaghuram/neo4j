/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.neo4j.bench.micro.config.Neo4jArchive;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.compressors.CompressorException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class Neo4jArchiveTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldExtractNeo4jWrapperFromArchiveAndParseJvmArgs()
            throws IOException, ArchiveException, CompressorException
    {
        try ( InputStream archive = getClass().getResource( "/neo4j-enterprise-3.1.0-M09-unix.tar.gz" ).openStream() )
        {
            Neo4jArchive neo4jArchive = new Neo4jArchive( archive );
            assertTrue( neo4jArchive.hasDefaultJvmArgs() );
            List<String> jvmArgs = neo4jArchive.defaultJvmArgs();
            System.out.println( jvmArgs );
            assertThat( jvmArgs.size(), equalTo( 8 ) );
        }
    }
}
