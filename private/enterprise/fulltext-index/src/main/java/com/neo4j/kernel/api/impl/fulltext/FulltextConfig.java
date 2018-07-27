/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Settings;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Configuration settings for the fulltext index.
 */
public class FulltextConfig implements LoadableConfig
{
    private static final String DEFAULT_ANALYZER = org.apache.lucene.analysis.standard.StandardAnalyzer.class.getName();

    @Description( "The fully qualified class name for the analyzer that the fulltext indexes should use by default." )
    public static final Setting<String> fulltext_default_analyzer = setting( "dbms.index.fulltext.default_analyzer", STRING, DEFAULT_ANALYZER );

    @Description( "Whether or not fulltext indexes should be eventually consistent by default or not." )
    public static final Setting<Boolean> eventually_consistent = setting( "dbms.index.fulltext.eventually_consistent", BOOLEAN, Settings.FALSE );
}
