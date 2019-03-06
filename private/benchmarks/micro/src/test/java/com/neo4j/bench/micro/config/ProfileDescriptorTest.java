/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.neo4j.bench.micro.profile.ProfileDescriptor.noProfile;
import static com.neo4j.bench.micro.profile.ProfileDescriptor.profileTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ProfileDescriptorTest
{
    @TempDir
    public Path temporaryFolder;

    @Test
    public void shouldSerializeNoProfile() throws IOException
    {
        shouldSerializeAndDeserialize( noProfile() );
    }

    @Test
    public void shouldSerializeWithProfile() throws IOException
    {
        shouldSerializeAndDeserialize(
                profileTo(
                        Files.createTempDirectory( temporaryFolder, "" ),
                        Lists.newArrayList( ProfilerType.JFR ) ) );
        shouldSerializeAndDeserialize(
                profileTo(
                        Files.createTempDirectory( temporaryFolder, "" ),
                        Lists.newArrayList( ProfilerType.JFR, ProfilerType.ASYNC ) ) );
    }

    private Object shouldSerializeAndDeserialize( Object before ) throws IOException
    {
        File jsonFile = Files.createTempFile( temporaryFolder, "", "" ).toFile();
        JsonUtil.serializeJson( jsonFile.toPath(), before );
        Object after = JsonUtil.deserializeJson( jsonFile.toPath(), before.getClass() );
        assertThat( before, equalTo( after ) );
        return after;
    }
}
