/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.assist;

import com.neo4j.bench.model.model.Parameters;

import java.util.List;

class CompositeInternalProfilerAssist implements InternalProfilerAssist
{

    private final Parameters ownParameters;
    private final List<InternalProfilerAssist> assists;

    CompositeInternalProfilerAssist( Parameters ownParameters, List<InternalProfilerAssist> assists )
    {
        this.ownParameters = ownParameters;
        this.assists = assists;
    }

    @Override
    public Parameters ownParameter()
    {
        return ownParameters;
    }

    @Override
    public void onWarmupBegin()
    {
        assists.forEach( InternalProfilerAssist::onWarmupBegin );
    }

    @Override
    public void onWarmupFinished()
    {
        assists.forEach( InternalProfilerAssist::onWarmupFinished );
    }

    @Override
    public void onMeasurementBegin()
    {
        assists.forEach( InternalProfilerAssist::onMeasurementBegin );
    }

    @Override
    public void onMeasurementFinished()
    {
        assists.forEach( InternalProfilerAssist::onMeasurementFinished );
    }
}
