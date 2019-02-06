/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.util.JsonUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static com.neo4j.bench.micro.profile.ProfileDescriptor.noProfile;
import static com.neo4j.bench.micro.profile.ProfileDescriptor.profileTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ProfileDescriptorTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
                        temporaryFolder.newFolder().toPath(),
                        Lists.newArrayList( ProfilerType.JFR ) ) );
        shouldSerializeAndDeserialize(
                profileTo(
                        temporaryFolder.newFolder().toPath(),
                        Lists.newArrayList( ProfilerType.JFR, ProfilerType.ASYNC ) ) );
    }

    private Object shouldSerializeAndDeserialize( Object before ) throws IOException
    {
        File jsonFile = temporaryFolder.newFile();
        JsonUtil.serializeJson( jsonFile.toPath(), before );
        Object after = JsonUtil.deserializeJson( jsonFile.toPath(), before.getClass() );
        assertThat( before, equalTo( after ) );
        return after;
    }
}
