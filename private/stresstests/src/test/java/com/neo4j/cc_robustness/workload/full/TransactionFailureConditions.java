/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

import com.neo4j.cc_robustness.ConnectionDisruptedException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jboss.netty.handler.queue.BlockingReadTimeoutException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.util.DebugUtil.StackTracer;

import static java.lang.String.format;
import static org.neo4j.internal.helpers.Exceptions.contains;
import static org.neo4j.internal.helpers.Exceptions.withMessage;

/**
 * Tries to gather all ways transactions and requests can fail in and able to decide on severity.
 * <p>
 * All errors in here should be looked over, and they should be revised to throw better user-understandable exceptions.
 */
public class TransactionFailureConditions implements Function<Throwable,TransactionOutcome>
{
    private final List<Condition> conditions = new ArrayList<>( Arrays.asList( new InstanceOf( TransactionOutcome.deadlock, DeadlockDetectedException.class ),
            new InstanceOf( TransactionOutcome.transient_failure, TransientFailureException.class ),
            new InstanceOf( TransactionOutcome.boring_failure, ConnectionDisruptedException.class ),
            new Contains( TransactionOutcome.boring_failure, "Database is currently not available", TransactionFailureException.class ),
            new Contains( TransactionOutcome.boring_failure, NotFoundException.class ),
            new Contains( TransactionOutcome.boring_failure, "Timeout waiting for database to become available",
                    org.neo4j.graphdb.TransactionFailureException.class ),
            new Condition( TransactionOutcome.benign_failure, "Slave acquire lock request timeout" )
            {
                @Override
                public boolean test( Throwable e )
                {
                    if ( ExceptionUtils.indexOfThrowable( e, BlockingReadTimeoutException.class ) != -1 && containsStackTraceElement( e, new Predicate<>()
                    {
                        private final Predicate<StackTraceElement> isLocksClient = classImplementingInterface( Locks.Client.class );
                        private final Predicate<StackTraceElement> isExclusiveLockCall = forMethod( "acquireExclusive" );
                        private final Predicate<StackTraceElement> isSharedLockCall = forMethod( "acquireShared" );

                        @Override
                        public boolean test( StackTraceElement t )
                        {
                            return isLocksClient.test( t ) && (isExclusiveLockCall.test( t ) || isSharedLockCall.test( t ));
                        }
                    } ) )
                    {
                        withMessage( e, "LOCK TIMEOUT | " + e.getMessage() );
                        return true;
                    }
                    return false;
                }
            },
            new Contains( TransactionOutcome.benign_failure, "The lock session requested to start is already in use", TransactionFailureException.class ) ) );
    private final StackTracer unknown = new StackTracer();

    private static Predicate<Throwable> exceptionsWithMessageContaining( final String message )
    {
        return item -> item.getMessage() != null && item.getMessage().contains( message );
    }

    private static Predicate<StackTraceElement> classImplementingInterface( final Class<?> cls )
    {
        return item ->
        {
            try
            {
                for ( Class<?> interfaceClass : Class.forName( item.getClassName() ).getInterfaces() )
                {
                    if ( interfaceClass.equals( cls ) )
                    {
                        return true;
                    }
                }
                return false;
            }
            catch ( ClassNotFoundException e )
            {
                return false;
            }
        };
    }

    private static boolean containsStackTraceElement( Throwable cause, final Predicate<StackTraceElement> predicate )
    {
        return contains( cause, item ->
        {
            for ( StackTraceElement element : item.getStackTrace() )
            {
                if ( predicate.test( element ) )
                {
                    return true;
                }
            }
            return false;
        } );
    }

    private static Predicate<StackTraceElement> forMethod( final String name )
    {
        return item -> item.getMethodName().equals( name );
    }

    @Override
    public synchronized TransactionOutcome apply( Throwable e )
    {
        for ( Condition condition : conditions )
        {
            if ( condition.test( e ) )
            {
                return condition.register();
            }
        }

        final AtomicInteger causeCount = unknown.add( e );
        if ( causeCount.get() == 1 )
        {
            conditions.add( new Condition( TransactionOutcome.unknown_failure, e.toString() )
            {
                @Override
                public boolean test( Throwable t )
                {
                    return false;
                }

                @Override
                protected int count()
                {
                    return causeCount.get();
                }
            } );
        }
        return TransactionOutcome.unknown_failure;
    }

    @Override
    public synchronized String toString()
    {
        List<Condition> copy = new ArrayList<>( conditions );
        Collections.sort( copy );
        StringBuilder builder = new StringBuilder();
        for ( Condition condition : copy )
        {
            if ( condition.count() == 0 )
            {
                break;
            }

            builder.append( "  " ).append( condition ).append( format( "%n" ) );
        }
        return builder.toString();
    }

    private abstract static class Condition implements Predicate<Throwable>, Comparable<Condition>
    {
        private final AtomicInteger count = new AtomicInteger();
        private final TransactionOutcome outcome;
        private final String name;

        Condition( TransactionOutcome outcome, String name )
        {
            this.outcome = outcome;
            this.name = name;
        }

        TransactionOutcome register()
        {
            count.incrementAndGet();
            return outcome;
        }

        protected int count()
        {
            return count.get();
        }

        @Override
        public String toString()
        {
            return count() + ": " + name;
        }

        @Override
        public int compareTo( Condition o )
        {
            return Integer.compare( o.count(), count() );
        }
    }

    private static class InstanceOf extends Condition
    {
        private final Class<? extends Throwable> cls;

        InstanceOf( TransactionOutcome outcome, Class<? extends Throwable> cls )
        {
            super( outcome, cls.getName() );
            this.cls = cls;
        }

        @Override
        public boolean test( Throwable t )
        {
            return cls.isAssignableFrom( t.getClass() );
        }
    }

    private static class Contains extends Condition
    {
        private final Predicate<Throwable>[] matches;

        Contains( TransactionOutcome outcome, String name, Predicate<Throwable>... matches )
        {
            super( outcome, name );
            this.matches = matches;
        }

        Contains( TransactionOutcome outcome, String message, Class<? extends Throwable> cls )
        {
            this( outcome, "*" + cls.getName() + " containing '" + message + "'", t -> exceptionsWithMessageContaining( message ).test( t ) &&
                    Predicates.instanceOfAny( cls ).test( t ) );
        }

        Contains( TransactionOutcome outcome, Class<? extends Throwable> cls )
        {
            this( outcome, "*" + cls.getName(), Predicates.instanceOfAny( cls ) );
        }

        @Override
        public boolean test( Throwable t )
        {
            for ( Predicate<Throwable> predicate : matches )
            {
                if ( Exceptions.contains( t, predicate ) )
                {
                    return true;
                }
            }
            return false;
        }
    }
}
