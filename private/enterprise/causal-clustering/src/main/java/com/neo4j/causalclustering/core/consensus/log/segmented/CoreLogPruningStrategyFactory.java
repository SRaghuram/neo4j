/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.neo4j.function.Factory;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.parse;

public class CoreLogPruningStrategyFactory implements Factory<CoreLogPruningStrategy>
{
    private final String pruningStrategyConfig;
    private final LogProvider logProvider;

    public CoreLogPruningStrategyFactory( String pruningStrategyConfig, LogProvider logProvider )
    {
        this.pruningStrategyConfig = pruningStrategyConfig;
        this.logProvider = logProvider;
    }

    @Override
    public CoreLogPruningStrategy newInstance()
    {
        ThresholdConfigParser.ThresholdConfigValue thresholdConfigValue = parse( pruningStrategyConfig );

        String type = thresholdConfigValue.type;
        long value = thresholdConfigValue.value;
        switch ( type )
        {
        case "size":
            return new SizeBasedLogPruningStrategy( value );
        case "txs":
        case "entries": // txs and entries are synonyms
            return new EntryBasedLogPruningStrategy( value, logProvider );
        case "hours": // hours and days are currently not supported as such, default to no prune
        case "days":
            throw new IllegalArgumentException(
                    "Time based pruning not supported yet for the segmented raft log, got '" + type + "'." );
        case "false":
            return new NoPruningPruningStrategy();
        default:
            throw new IllegalArgumentException( "Invalid log pruning configuration value '" + value +
                    "'. Invalid type '" + type + "', valid are files, size, txs, entries, hours, days." );
        }
    }
}
