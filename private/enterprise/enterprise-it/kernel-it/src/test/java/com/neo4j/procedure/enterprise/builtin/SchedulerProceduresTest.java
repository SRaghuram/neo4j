/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.scheduler.GroupedDaemonThreadFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.scheduler.Group.RAFT_CLIENT;
import static org.neo4j.scheduler.Group.RAFT_SERVER;

@EnterpriseDbmsExtension
class SchedulerProceduresTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldListActiveGroups()
    {
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( "CALL dbms.scheduler.groups" ) )
            {
                assertTrue( result.hasNext() );
                while ( result.hasNext() )
                {
                    assertThat( (Long) result.next().get( "threads" ) ).isGreaterThan( 0L );
                }
            }
            tx.commit();
        }
    }

    @Test
    void shouldProfileGroup()
    {
        try ( Transaction tx = db.beginTx() )
        {
            String result = tx.execute( "CALL dbms.scheduler.profile('sample', 'PageCacheEviction', '5s')" ).resultAsString();
            assertThat( result ).contains( "MuninnPageCache.parkUntilEvictionRequired" );
            tx.commit();
        }
    }

    @Test
    void testListJobs() throws InterruptedException
    {
        var scheduler = db.getDependencyResolver().resolveDependency( JobScheduler.class );

        var jobLatch = new BinaryLatch();
        var jobStartLatch = new CountDownLatch( 2 );
        try
        {
            scheduler.setThreadFactory( RAFT_SERVER, ( group, parent ) -> new GroupedDaemonThreadFactory( group, parent )
            {
                @Override
                public Thread newThread( Runnable job )
                {
                    return super.newThread( () ->
                    {
                        jobLatch.await();
                        job.run();
                    } );
                }
            } );

            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( "user 1", "db 1", "job 101" ), () ->
            {
            } );
            scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( null, null, "job 102" ), () ->
            {
                jobStartLatch.countDown();
                jobLatch.await();
            } );
            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( "user 2", "db 2", "job 103" ), () -> 1 );
            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( null, null, "job 104" ), () ->
            {
            } ).cancel();

            try
            {
                scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( null, null, "job 105" ), () ->
                {
                } ).get();
            }
            catch ( Exception e )
            {
                fail( e );
            }

            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( "user 3", "db 3", "job 106" ), () ->
            {
            }, 1, HOURS );
            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( null, null, "job 107" ), () ->
            {
            }, 0, HOURS );
            scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( null, null, "job 108" ), () ->
            {
                jobStartLatch.countDown();
                jobLatch.await();
            }, 1, TimeUnit.NANOSECONDS );
            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( null, null, "job 109" ), () ->
            {
            }, 1, HOURS ).cancel();

            scheduler.scheduleRecurring( RAFT_SERVER, new JobMonitoringParams( null, null, "job 110" ), () ->
            {
            }, 1, HOURS );
            scheduler.scheduleRecurring( RAFT_SERVER, new JobMonitoringParams( null, null, "job 111" ), () ->
            {
            }, 1, 1, HOURS );

            assertTrue( jobStartLatch.await( 10, TimeUnit.SECONDS ) );

            try ( Transaction tx = db.beginTx() )
            {
                var result = tx.execute( "CALL dbms.scheduler.jobs" );
                var jobs = mapJobStatus( result );

                assertJob( jobs, "RaftServer", "user 1", "db 1", "job 101", "IMMEDIATE", "ENQUEUED", false, false );
                assertJob( jobs, "RaftClient", "", "", "job 102", "IMMEDIATE", "EXECUTING", false, false );
                assertJob( jobs, "RaftServer", "user 2", "db 2", "job 103", "IMMEDIATE", "ENQUEUED", false, false );
                assertFalse( jobs.containsKey( "job 104" ) );
                assertFalse( jobs.containsKey( "job 105" ) );
                assertJob( jobs, "RaftServer", "user 3", "db 3", "job 106", "DELAYED", "SCHEDULED", true, false );
                assertJob( jobs, "RaftServer", "", "", "job 107", "DELAYED", "ENQUEUED", true, false );
                assertJob( jobs, "RaftClient", "", "", "job 108", "DELAYED", "EXECUTING", true, false );
                assertFalse( jobs.containsKey( "job 9" ) );
                assertJob( jobs, "RaftServer", "", "", "job 110", "PERIODIC", "ENQUEUED", true, true );
                assertJob( jobs, "RaftServer", "", "", "job 111", "PERIODIC", "SCHEDULED", true, true );
            }
        }
        finally
        {
            jobLatch.release();
        }
    }

    @Test
    void testListFailedJobRuns() throws InterruptedException
    {
        var scheduler = db.getDependencyResolver().resolveDependency( JobScheduler.class );

        scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( "user 1", "db 1", "job 201" ), () ->
        {
            throw new TestException( "something went wrong 1" );
        } );
        awaitFailure( scheduler, 1 );
        scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( null, null, "job 202" ), () ->
        {
            throw new TestException( "something went wrong 2" );
        } );
        awaitFailure( scheduler, 2 );
        scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( "user 2", "db 2", "job 203" ), () ->
        {
            throw new TestException( "something went wrong 4" );
        }, 0, HOURS );
        awaitFailure( scheduler, 3 );
        scheduler.scheduleRecurring( RAFT_CLIENT, new JobMonitoringParams( "user 3", "db 3", "job 204" ), () ->
        {
            throw new TestException( "something went wrong 5" );
        }, 0, 1, HOURS );
        awaitFailure( scheduler, 4 );

        try ( Transaction tx = db.beginTx() )
        {
            var result = tx.execute( "CALL dbms.scheduler.failedJobRuns" );
            var jobs = runJobFailure( result );

            assertFailure( jobs, "RaftClient", "user 1", "db 1", "job 201", "IMMEDIATE", "TestException: something went wrong 1" );
            assertFailure( jobs, "RaftClient", "", "", "job 202", "IMMEDIATE", "TestException: something went wrong 2" );
            assertFailure( jobs, "RaftClient", "user 2", "db 2", "job 203", "DELAYED", "TestException: something went wrong 4" );
            assertFailure( jobs, "RaftClient", "user 3", "db 3", "job 204", "PERIODIC", "TestException: something went wrong 5" );
        }
    }

    private void awaitFailure( JobScheduler jobScheduler, int expected ) throws InterruptedException
    {
        var start = Instant.now();

        while ( true )
        {
            if ( jobScheduler.getFailedJobRuns().size() == expected )
            {
                return;
            }

            if ( Duration.between( start, Instant.now() ).toMinutes() > 1 )
            {
                fail();
            }

            Thread.sleep( 1 );
        }
    }

    private Map<String,JobStatusRecord> mapJobStatus( Result result )
    {
        Map<String,JobStatusRecord> jobRecords = new HashMap<>();

        try
        {
            result.accept( (Result.ResultVisitor<Exception>) row ->
            {
                var group = row.getString( "group" );
                var database = row.getString( "database" );
                var submitter = row.getString( "submitter" );
                var description = row.getString( "description" );
                var type = row.getString( "type" );
                var scheduledAt = !row.getString( "scheduledAt" ).isEmpty();
                var period = !row.getString( "period" ).isEmpty();
                var status = row.getString( "state" );

                assertFalse( row.getString( "submitted" ).isEmpty() );

                jobRecords.put( description, new JobStatusRecord( group, submitter, database, description, type, status, scheduledAt, period ) );

                return true;
            } );
        }
        catch ( Exception e )
        {
            fail( e );
        }

        return jobRecords;
    }

    private Map<String,JobRunFailureRecord> runJobFailure( Result result )
    {
        Map<String,JobRunFailureRecord> runFailures = new HashMap<>();

        try
        {
            result.accept( (Result.ResultVisitor<Exception>) row ->
            {
                var group = row.getString( "group" );
                var database = row.getString( "database" );
                var submitter = row.getString( "submitter" );
                var description = row.getString( "description" );
                var type = row.getString( "type" );
                var failureDescription = row.getString( "failureDescription" );

                assertFalse( row.getString( "submitted" ).isEmpty() );
                assertFalse( row.getString( "executionStart" ).isEmpty() );
                assertFalse( row.getString( "failureTime" ).isEmpty() );

                runFailures.put( description, new JobRunFailureRecord( group, database, submitter, description, type, failureDescription ) );

                return true;
            } );
        }
        catch ( Exception e )
        {
            fail( e );
        }

        return runFailures;
    }

    private void assertJob( Map<String,JobStatusRecord> jobRecords, String group, String submitter, String database, String description, String type,
            String status, boolean scheduledAtPresent, boolean periodPresent )
    {
        var job = jobRecords.get( description );
        assertNotNull( job );
        assertEquals( group, job.group );
        assertEquals( submitter, job.submitter );
        assertEquals( database, job.database );
        assertEquals( description, job.description );
        assertEquals( type, job.type );
        assertEquals( scheduledAtPresent, job.scheduledAtPresent );
        assertEquals( periodPresent, job.periodPresent );
        assertEquals( status, job.status );
    }

    private void assertFailure( Map<String,JobRunFailureRecord> jobRecords, String group, String submitter, String database, String description, String type,
            String failureDescription )
    {
        var job = jobRecords.get( description );
        assertNotNull( job );
        assertEquals( group, job.group );
        assertEquals( submitter, job.submitter );
        assertEquals( database, job.database );
        assertEquals( description, job.description );
        assertEquals( type, job.type );
        assertEquals( failureDescription, job.failureDescription );
    }

    private static class JobStatusRecord
    {
        private final String group;
        private final String database;
        private final String submitter;
        private final String description;
        private final String type;
        private final String status;
        private final boolean scheduledAtPresent;
        private final boolean periodPresent;

        JobStatusRecord( String group, String submitter, String database, String description, String type, String status, boolean scheduledAtPresent,
                boolean periodPresent )
        {
            this.group = group;
            this.database = database;
            this.submitter = submitter;
            this.description = description;
            this.type = type;
            this.scheduledAtPresent = scheduledAtPresent;
            this.periodPresent = periodPresent;
            this.status = status;
        }
    }

    private static class JobRunFailureRecord
    {
        private final String group;
        private final String database;
        private final String submitter;
        private final String description;
        private final String type;
        private final String failureDescription;

        JobRunFailureRecord( String group, String database, String submitter, String description, String type,
                String failureDescription )
        {
            this.group = group;
            this.database = database;
            this.submitter = submitter;
            this.description = description;
            this.type = type;
            this.failureDescription = failureDescription;
        }
    }

    private static class TestException extends RuntimeException
    {

        TestException( String message )
        {
            super( message );
        }
    }
}
