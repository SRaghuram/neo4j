/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = IndexSamplingManager.NAME )
@Description( "Handle index sampling." )
@Deprecated
public interface IndexSamplingManager
{
    String NAME = "Index sampler";

    @Description( "Trigger index sampling for the index associated with the provided label and property key." +
            " If forceSample is set to true an index sampling will always happen otherwise a sampling is only " +
            "done if the number of updates exceeds the configured dbms.index_sampling.update_percentage." )
    void triggerIndexSampling( String labelKey, String propertyKey, boolean forceSample );
}
