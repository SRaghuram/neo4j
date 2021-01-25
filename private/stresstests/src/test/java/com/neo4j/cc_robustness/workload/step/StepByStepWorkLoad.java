/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.step;

import com.neo4j.cc_robustness.CcInstance;
import com.neo4j.cc_robustness.Orchestrator;
import com.neo4j.cc_robustness.workload.GraphOperations;
import com.neo4j.cc_robustness.workload.ShutdownType;
import com.neo4j.cc_robustness.workload.WorkLoad;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.DbRepresentation;

public class StepByStepWorkLoad implements WorkLoad
{
    private final int assumedCcClusterSize;
    private final Step[] steps;
    private final Step toDoInBetween;
    private final long millisInBetween;

    public StepByStepWorkLoad( int assumedCcClusterSize, long millisInBetween, Step toDoInBetween, Step... steps )
    {
        this.assumedCcClusterSize = assumedCcClusterSize;
        this.millisInBetween = millisInBetween;
        this.toDoInBetween = toDoInBetween;
        this.steps = steps;
    }

    public static Step[] parse( String string )
    {
        List<Step> steps = new ArrayList<>();
        for ( String token : string.split( Pattern.quote( " " ) ) )
        {
            String[] parts = token.split( Pattern.quote( "," ) );
            String type = parts[0];
            Integer retryCount = null;
            if ( type.startsWith( "[" ) )
            {
                int endIndex = type.indexOf( ']' );
                retryCount = Integer.parseInt( type.substring( 1, endIndex ) );
                type = type.substring( endIndex + 1 );
            }
            int serverId = Integer.parseInt( parts[1] );
            Step step;
            if ( type.equals( Op.class.getSimpleName() ) )
            {
                step = new Op( serverId, GraphOperations.Operation.valueOf( parts[2] ) );
            }
            else if ( type.equals( Up.class.getSimpleName() ) )
            {
                step = new Up( serverId );
            }
            else if ( type.equals( Down.class.getSimpleName() ) )
            {
                step = new Down( serverId, ShutdownType.valueOf( parts[2] ) );
            }
            else
            {
                throw new IllegalArgumentException( "Unknown type '" + type + "'" );
            }
            if ( retryCount != null )
            {
                step = retriable( step, retryCount );
            }
            steps.add( step );
        }
        return steps.toArray( new Step[steps.size()] );
    }

    public static Step retriable( final Step step, final int maxNumberOfTimes )
    {
        return new Step()
        {
            @Override
            public void perform( final StepByStepWorkLoad workLoad, Orchestrator orchestrator, Log log ) throws Exception
            {
                Exception exception = null;
                for ( int i = 0; i < maxNumberOfTimes; i++ )
                {
                    try
                    {
                        step.perform( workLoad, orchestrator, log );
                        exception = null;
                        break;
                    }
                    catch ( Exception e )
                    {
                        exception = e;
                        log.warn( "Retrying because of:" + e.toString() );
                        e.printStackTrace();
                        Thread.sleep( workLoad.millisInBetween * 2 );
                    }
                }
                if ( exception != null )
                {
                    throw exception;
                }
            }

            @Override
            public String toString()
            {
                return "[" + maxNumberOfTimes + "]" + step.toString();
            }
        };
    }

    @Override
    public void perform( Orchestrator orchestrator, LogProvider logProvider ) throws Exception
    {
        Log log = logProvider.getLog( getClass() );
        log.info( "Performing (" + steps.length + ") steps: " + stepsToString() );
        if ( orchestrator.getNumberOfCcInstances() != assumedCcClusterSize )
        {
            throw new IllegalStateException( "Must be a CC cluster size of " + assumedCcClusterSize );
        }

        int counter = 0;
        try
        {
            for ( Step step : steps )
            {
                log.info( "(" + ++counter + ") " + step );
                step.perform( this, orchestrator, log );
                log.info( orchestrator.getConvenientStatus() );
                Thread.sleep( millisInBetween );
                if ( toDoInBetween != null )
                {
                    toDoInBetween.perform( this, orchestrator, log );
                    Thread.sleep( millisInBetween );
                }
            }
        }
        catch ( Exception e )
        {
            log.warn( "Those steps ended in branching of data, just ignore it " + "(it's a bit hard to prevent when generating random steps)" );
        }
    }

    private String stepsToString()
    {
        StringBuilder result = new StringBuilder();
        for ( Step step : steps )
        {
            result.append( step.toString() ).append( " " );
        }
        return result.toString();
    }

    @Override
    public void forceShutdown()
    {
        // Do nothing
    }

    public interface Step
    {
        void perform( StepByStepWorkLoad workLoad, Orchestrator orchestrator, Log log ) throws Exception;
    }

    public abstract static class SpecificInstanceStep implements Step
    {
        private final int serverId;

        public SpecificInstanceStep( int serverId )
        {
            this.serverId = serverId;
        }

        protected int getServerId()
        {
            return serverId;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "," + serverId;
        }
    }

    public static class Op extends SpecificInstanceStep
    {
        private final GraphOperations.Operation[] operation;

        public Op( int serverId, GraphOperations.Operation operation )
        {
            super( serverId );
            this.operation = operation != null ? new GraphOperations.Operation[]{operation} : new GraphOperations.Operation[0];
        }

        @Override
        public void perform( StepByStepWorkLoad workLoad, Orchestrator orchestrator, Log log ) throws Exception
        {
            CcInstance ccInstance = orchestrator.getCcInstance( getServerId() );
            ccInstance.doBatchOfOperations( 1, this.operation );
        }

        @Override
        public String toString()
        {
            return super.toString() + "," + operation[0].name();
        }
    }

    public static class Down extends SpecificInstanceStep
    {
        private final ShutdownType type;

        public Down( int serverId, ShutdownType type )
        {
            super( serverId );
            this.type = type;
        }

        @Override
        public void perform( StepByStepWorkLoad workLoad, Orchestrator orchestrator, Log log ) throws Exception
        {
            orchestrator.shutdownCcInstance( getServerId(), type, false );
        }

        @Override
        public String toString()
        {
            return super.toString() + "," + type;
        }
    }

    public static class Up extends SpecificInstanceStep
    {
        public Up( int serverId )
        {
            super( serverId );
        }

        @Override
        public void perform( StepByStepWorkLoad workLoad, Orchestrator orchestrator, Log log ) throws Exception
        {
            orchestrator.startCcInstance( getServerId() );
        }
    }

    public static class VerifyConsistency implements Step
    {
        @Override
        public void perform( StepByStepWorkLoad workLoad, Orchestrator orchestrator, Log log ) throws Exception
        {
            for ( int i = 0; i < orchestrator.getNumberOfCcInstances(); i++ )
            {
                CcInstance instance = orchestrator.getCcInstance( i );
                // TODO use consistency checker?
                if ( instance != null )
                {
                    instance.representation();
                }
            }
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }
}
