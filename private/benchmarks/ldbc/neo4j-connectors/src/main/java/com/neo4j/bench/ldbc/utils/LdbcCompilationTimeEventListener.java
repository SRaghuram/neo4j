/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.utils;

import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer;
import org.neo4j.cypher.internal.tracing.TimingCompilationTracer;

public class LdbcCompilationTimeEventListener implements TimingCompilationTracer.EventListener
{
    private final ConcurrentHashMap<String,Long> parsingTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,Long> astRewritingTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,Long> semanticCheckTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,Long> logicalPlanTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,Long> executionPlanTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,Long> totalCompilationTimes = new ConcurrentHashMap<>();

    @Override
    public void queryCompiled( TimingCompilationTracer.QueryEvent queryEvent )
    {
        if ( totalCompilationTimes.containsKey( queryEvent.query() ) )
        {
            throw new RuntimeException( "Compilation Time for " + queryEvent.query() + " already recorded" );
        }
        totalCompilationTimes.put( queryEvent.query(), queryEvent.nanoTime() );

        for ( TimingCompilationTracer.PhaseEvent phaseEvent : queryEvent.phases() )
        {
            CompilationPhaseTracer.CompilationPhase compilationPhase = phaseEvent.phase();
            switch ( compilationPhase )
            {
            case AST_REWRITE:
            {
                long nanoTimeSoFar =
                        (astRewritingTimes.contains( queryEvent.query() )) ? astRewritingTimes.get( queryEvent.query() )
                                                                           : 0;
                astRewritingTimes.put( queryEvent.query(), nanoTimeSoFar + phaseEvent.nanoTime() );
                break;
            }
            case CODE_GENERATION:
            {
                long nanoTimeSoFar = (executionPlanTimes.contains( queryEvent.query() )) ? executionPlanTimes
                        .get( queryEvent.query() ) : 0;
                executionPlanTimes.put( queryEvent.query(), nanoTimeSoFar + phaseEvent.nanoTime() );
                break;
            }
            case LOGICAL_PLANNING:
            {
                long nanoTimeSoFar =
                        (logicalPlanTimes.contains( queryEvent.query() )) ? logicalPlanTimes.get( queryEvent.query() )
                                                                          : 0;
                logicalPlanTimes.put( queryEvent.query(), nanoTimeSoFar + phaseEvent.nanoTime() );
                break;
            }
            case PARSING:
            {
                long nanoTimeSoFar =
                        (parsingTimes.contains( queryEvent.query() )) ? parsingTimes.get( queryEvent.query() ) : 0;
                parsingTimes.put( queryEvent.query(), nanoTimeSoFar + phaseEvent.nanoTime() );
                break;
            }
            case PIPE_BUILDING:
            {
                long nanoTimeSoFar = (executionPlanTimes.contains( queryEvent.query() )) ? executionPlanTimes
                        .get( queryEvent.query() ) : 0;
                executionPlanTimes.put( queryEvent.query(), nanoTimeSoFar + phaseEvent.nanoTime() );
                break;
            }
            case SEMANTIC_CHECK:
            {
                long nanoTimeSoFar = (semanticCheckTimes.contains( queryEvent.query() )) ? semanticCheckTimes
                        .get( queryEvent.query() ) : 0;
                semanticCheckTimes.put( queryEvent.query(), nanoTimeSoFar + phaseEvent.nanoTime() );
                break;
            }
            default:
            {
                throw new RuntimeException( "Unrecognized compilation phase " + compilationPhase.name() );
            }
            }
        }
    }

    public long getParsingTimeElapsed( String s )
    {
        return (parsingTimes.containsKey( s )) ? parsingTimes.get( s ) : -1;
    }

    public long getAstRewritingTimeElapsed( String s )
    {
        return (astRewritingTimes.containsKey( s )) ? astRewritingTimes.get( s ) : -1;
    }

    public long getSemanticCheckTimeElapsed( String s )
    {
        return (semanticCheckTimes.containsKey( s )) ? semanticCheckTimes.get( s ) : -1;
    }

    public long getLogicalPlanTimeElapsed( String s )
    {
        return (logicalPlanTimes.containsKey( s )) ? logicalPlanTimes.get( s ) : -1;
    }

    public long getExecutionPlanTimeElapsed( String s )
    {
        return (executionPlanTimes.containsKey( s )) ? executionPlanTimes.get( s ) : -1;
    }

    public long getTotalTime( String s )
    {
        return (totalCompilationTimes.containsKey( s )) ? totalCompilationTimes.get( s ) : -1;
    }
}
