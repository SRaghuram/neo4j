/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom.surface;

import java.time.Duration;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Settings;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Configuration parameters for the bloom fulltext addon.
 */
public class BloomFulltextConfig implements LoadableConfig
{
    @Description( "Enable the fulltext addon for bloom." )
    @Internal
    static final Setting<Boolean> bloom_enabled = setting( "unsupported.dbms.bloom_enabled", BOOLEAN, FALSE );

    @Description( "Time between bloom index refreshes. Setting this to a low value will decrease performance." )
    @Internal
    static final Setting<Duration> bloom_refresh_delay = setting( "unsupported.dbms.bloom_refresh_delay", Settings.DURATION, "1h" );

    @Description( "Define the analyzer to use for the bloom index. Expects the fully qualified classname of the " +
                  "analyzer to use" )
    @Internal
    static final Setting<String> bloom_default_analyzer = setting( "unsupported.dbms.bloom_default_analyzer", STRING,
            "org.apache.lucene.analysis.standard.StandardAnalyzer" );
}
