/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InfraNamesHelperTest
{
    @Test
    public void sanitizeDockerImageTag()
    {
        //given
        String tag = "oracle-8";
        //when
        String actual = InfraNamesHelper.sanitizeDockerImageTag( tag );
        // then
        assertEquals( "oracle_8", actual );
    }

    @Test
    public void sanitizeJobName()
    {
        //given
        String tag = "macro-job-accesscontrol-user-v3.5";
        //when
        String actual = InfraNamesHelper.sanitizeDockerImageTag( tag );
        // then
        assertEquals( "macro_job_accesscontrol_user_v3_5", actual );
    }

    @Test
    public void sanitizeJobDefinitionName()
    {
        //given
        String tag = "macro-job-accesscontrol-user-v3.5-job-name";
        //when
        String actual = InfraNamesHelper.sanitizeDockerImageTag( tag );
        // then
        assertEquals( "macro_job_accesscontrol_user_v3_5_job_name", actual );
    }
}
