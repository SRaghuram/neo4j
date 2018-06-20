/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Configuration parameters for the fulltext index.
 */
public class FulltextConfig implements LoadableConfig
{
    @Description( "Define the analyzer to use for the fulltext index. Expects the fully qualified classname of the analyzer to use. " +
            "WARNING: changing this property will trigger re-population of all existing fulltext indexes." )
    public static final Setting<String> fulltext_default_analyzer =
            setting( "dbms.fulltext_default_analyzer", STRING, "org.apache.lucene.analysis.standard.StandardAnalyzer" );
}
