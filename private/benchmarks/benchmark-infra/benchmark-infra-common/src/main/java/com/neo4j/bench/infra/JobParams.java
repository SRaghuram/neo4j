/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.common.util.JsonUtil;
import com.neo4j.bench.infra.commands.InfraParams;

import java.util.Objects;

public class JobParams
{

    public static JobParams fromJson( String json )
    {
        return JsonUtil.deserializeJson( json, JobParams.class );
    }

    private InfraParams infraParams;
    private RunWorkloadParams runWorkloadParams;

    // needed for JSON serialization
    private JobParams()
    {
    }

    public JobParams( InfraParams infraParams, RunWorkloadParams runWorkloadParams )
    {
        this.infraParams = infraParams;
        this.runWorkloadParams = runWorkloadParams;
    }

    public InfraParams infraParams()
    {
        return infraParams;
    }

    public RunWorkloadParams runWorkloadParams()
    {
        return runWorkloadParams;
    }

    public String toJson()
    {
        return JsonUtil.serializeJson( this );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        JobParams jobParams = (JobParams) o;
        return Objects.equals( infraParams, jobParams.infraParams ) &&
               Objects.equals( runWorkloadParams, jobParams.runWorkloadParams );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( infraParams, runWorkloadParams );
    }
}
