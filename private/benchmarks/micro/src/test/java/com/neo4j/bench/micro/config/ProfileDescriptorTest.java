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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.client.util.TestDirectorySupport.createTempDirectoryPath;
import static com.neo4j.bench.client.util.TestDirectorySupport.createTempFile;
import static com.neo4j.bench.micro.profile.ProfileDescriptor.noProfile;
import static com.neo4j.bench.micro.profile.ProfileDescriptor.profileTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ExtendWith( TestDirectoryExtension.class )
public class ProfileDescriptorTest
{
    @Inject
    public TestDirectory temporaryFolder;

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
                        createTempDirectoryPath( temporaryFolder.absolutePath() ),
                        Lists.newArrayList( ProfilerType.JFR ) ) );
        shouldSerializeAndDeserialize(
                profileTo(
                        createTempDirectoryPath( temporaryFolder.absolutePath() ),
                        Lists.newArrayList( ProfilerType.JFR, ProfilerType.ASYNC ) ) );
    }

    private Object shouldSerializeAndDeserialize( Object before ) throws IOException
    {
        File jsonFile = createTempFile( temporaryFolder.absolutePath() );
        JsonUtil.serializeJson( jsonFile.toPath(), before );
        Object after = JsonUtil.deserializeJson( jsonFile.toPath(), before.getClass() );
        assertThat( before, equalTo( after ) );
        return after;
    }
}
