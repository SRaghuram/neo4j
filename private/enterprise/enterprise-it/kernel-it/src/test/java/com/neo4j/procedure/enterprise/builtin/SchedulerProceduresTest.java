/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.common.Subject;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.scheduler.GroupedDaemonThreadFactory;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.BasicLoginContext;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.scheduler.Group.RAFT_CLIENT;
import static org.neo4j.scheduler.Group.RAFT_SERVER;

@EnterpriseDbmsExtension
class SchedulerProceduresTest
{
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private JobScheduler scheduler;

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
    void testListJobs() throws Exception
    {
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

            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( new Subject( "user 1" ), "db 1", "job 101",
                    () -> "Pretty impressive progress" ), () -> {} );
            scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( Subject.AUTH_DISABLED, null, "job 102" ), () ->
            {
                jobStartLatch.countDown();
                jobLatch.await();
            } );
            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( new Subject( "user 2" ), "db 2", "job 103" ), () -> 1 );
            scheduler.schedule( RAFT_SERVER, JobMonitoringParams.systemJob( "job 104" ), () -> { } ).cancel();

            scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( Subject.SYSTEM, null, "job 105" ), () -> { } ).get();

            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( new Subject( "user 3" ), "db 3", "job 106" ), () -> { }, 1, HOURS );
            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( Subject.AUTH_DISABLED, null, "job 107" ), () -> { }, 0, HOURS );
            scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( Subject.SYSTEM, null, "job 108" ), () ->
            {
                jobStartLatch.countDown();
                jobLatch.await();
            }, 1, TimeUnit.NANOSECONDS );
            scheduler.schedule( RAFT_SERVER, new JobMonitoringParams( Subject.SYSTEM, null, "job 109" ), () -> { }, 1, HOURS ).cancel();

            scheduler.scheduleRecurring( RAFT_SERVER, new JobMonitoringParams( Subject.SYSTEM, null, "job 110" ), () -> { }, 1, HOURS );
            scheduler.scheduleRecurring( RAFT_SERVER, new JobMonitoringParams( Subject.SYSTEM, null, "job 111" ), () -> { }, 1, 1, HOURS );

            assertTrue( jobStartLatch.await( 10, TimeUnit.SECONDS ) );

            try ( Transaction tx = db.beginTx() )
            {
                var result = tx.execute( "CALL dbms.scheduler.jobs" );
                var jobs = mapJobStatus( result );

                assertJob( jobs, "RaftServer", "user 1", "db 1", "job 101", "IMMEDIATE", "SCHEDULED", "Pretty impressive progress", false, false );
                assertJob( jobs, "RaftClient", "", "", "job 102", "IMMEDIATE", "EXECUTING", "", false, false );
                assertJob( jobs, "RaftServer", "user 2", "db 2", "job 103", "IMMEDIATE", "SCHEDULED", "", false, false );
                assertFalse( jobs.containsKey( "job 104" ) );
                assertFalse( jobs.containsKey( "job 105" ) );
                assertJob( jobs, "RaftServer", "user 3", "db 3", "job 106", "DELAYED", "SCHEDULED", "", true, false );
                assertJob( jobs, "RaftServer", "", "", "job 107", "DELAYED", "SCHEDULED", "", true, false );
                assertJob( jobs, "RaftClient", "", "", "job 108", "DELAYED", "EXECUTING", "", true, false );
                assertFalse( jobs.containsKey( "job 9" ) );
                assertJob( jobs, "RaftServer", "", "", "job 110", "PERIODIC", "SCHEDULED", "", true, true );
                assertJob( jobs, "RaftServer", "", "", "job 111", "PERIODIC", "SCHEDULED", "", true, true );

                assertJobIdsAreUnique( jobs );
            }
        }
        finally
        {
            jobLatch.release();
        }
    }

    @Test
    void testListFailedJobRuns() throws Exception
    {
        scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( new Subject( "user 1" ), "db 1", "job 201" ), () ->
        {
            throw new TestException( "something went wrong 1" );
        } );
        awaitFailure( scheduler, 1 );
        scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( Subject.SYSTEM, null, "job 202" ), () ->
        {
            throw new TestException( "something went wrong 2" );
        } );
        awaitFailure( scheduler, 2 );
        scheduler.schedule( RAFT_CLIENT, new JobMonitoringParams( new Subject( "user 2" ), "db 2", "job 203" ), () ->
        {
            throw new TestException( "something went wrong 4" );
        }, 0, HOURS );
        awaitFailure( scheduler, 3 );
        scheduler.scheduleRecurring( RAFT_CLIENT, new JobMonitoringParams( new Subject( "user 3" ), "db 3", "job 204" ), () ->
        {
            throw new TestException( "something went wrong 5" );
        }, 0, 1, HOURS );
        awaitFailure( scheduler, 4 );

        try ( Transaction tx = db.beginTx() )
        {
            var result = tx.execute( "CALL dbms.scheduler.failedJobs" );
            var jobs = runJobFailure( result );

            assertFailure( jobs, "RaftClient", "user 1", "db 1", "job 201", "IMMEDIATE", "TestException: something went wrong 1" );
            assertFailure( jobs, "RaftClient", "", "", "job 202", "IMMEDIATE", "TestException: something went wrong 2" );
            assertFailure( jobs, "RaftClient", "user 2", "db 2", "job 203", "DELAYED", "TestException: something went wrong 4" );
            assertFailure( jobs, "RaftClient", "user 3", "db 3", "job 204", "PERIODIC", "TestException: something went wrong 5" );
        }
    }

    @Test
    void testIndexPopulationJobMonitoring() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }

        var monitoringBlocker = new BinaryLatch();
        var indexBlocker = new BinaryLatch();
        IndexingService.MonitorAdapter blockingMonitor = new IndexingService.MonitorAdapter()
        {
            @Override
            public void indexPopulationScanStarting()
            {
                monitoringBlocker.release();
                indexBlocker.await();
            }
        };

        Monitors monitors = db.getDependencyResolver().resolveDependency( Monitors.class );
        monitors.addMonitorListener( blockingMonitor );

        try
        {
            scheduler.schedule( Group.TESTING, JobMonitoringParams.NOT_MONITORED, () ->
            {
                var logicContext = new BasicLoginContext( new User.Builder( "John Doe", null ).build(), SUCCESS );
                try ( Transaction tx = db.beginTransaction( EXPLICIT, logicContext ) )
                {
                    tx.execute( "CREATE INDEX myIndex FOR (n:Person) ON (n.name)" );
                    tx.commit();
                }
            } );

            monitoringBlocker.await();

            try ( Transaction tx = db.beginTx() )
            {
                var result = tx.execute( "CALL dbms.scheduler.jobs" );
                var jobs = mapJobStatus( result );

                assertJob( jobs, "IndexPopulationMain", "John Doe", db.databaseName(), "Population of index 'myIndex'", "IMMEDIATE", "EXECUTING",
                        "Total progress: 0.0%", false, false );
            }
        }
        finally
        {
            indexBlocker.release();
        }
    }

    private void awaitFailure( JobScheduler jobScheduler, int expected )
    {
        while ( jobScheduler.getFailedJobRuns().size() != expected )
        {
            LockSupport.parkNanos( MILLISECONDS.toNanos( 10 ) );
        }
    }

    private Map<String,JobStatusRecord> mapJobStatus( Result result ) throws Exception
    {
        Map<String,JobStatusRecord> jobRecords = new HashMap<>();

        result.accept( (Result.ResultVisitor<Exception>) row ->
        {
            var id = row.getString( "jobId" );
            var group = row.getString( "group" );
            var database = row.getString( "database" );
            var submitter = row.getString( "submitter" );
            var description = row.getString( "description" );
            var type = row.getString( "type" );
            var scheduledAt = !row.getString( "scheduledAt" ).isEmpty();
            var period = !row.getString( "period" ).isEmpty();
            var status = row.getString( "state" );
            var currentStateDescription = row.getString( "currentStateDescription" );

            assertFalse( row.getString( "submitted" ).isEmpty() );

            jobRecords.put( description,
                    new JobStatusRecord( id, group, submitter, database, description, type, status, currentStateDescription, scheduledAt, period ) );

            return true;
        } );
        return jobRecords;
    }

    private Map<String,JobRunFailureRecord> runJobFailure( Result result ) throws Exception
    {
        Map<String,JobRunFailureRecord> runFailures = new HashMap<>();

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

        return runFailures;
    }

    private void assertJobIdsAreUnique( Map<String,JobStatusRecord> jobRecords )
    {
        assertEquals( jobRecords.size(), jobRecords.values().stream().distinct().count() );
    }

    private void assertJob( Map<String,JobStatusRecord> jobRecords, String group, String submitter, String database, String description, String type,
            String status, String currentStateDescription, boolean scheduledAtPresent, boolean periodPresent )
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
        assertEquals( currentStateDescription, job.currentStateDescription );
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
        private final String id;
        private final String group;
        private final String database;
        private final String submitter;
        private final String description;
        private final String type;
        private final String status;
        private final String currentStateDescription;
        private final boolean scheduledAtPresent;
        private final boolean periodPresent;

        JobStatusRecord( String id, String group, String submitter, String database, String description, String type, String status,
                String currentStateDescription, boolean scheduledAtPresent, boolean periodPresent )
        {
            this.id = id;
            this.group = group;
            this.database = database;
            this.submitter = submitter;
            this.description = description;
            this.type = type;
            this.scheduledAtPresent = scheduledAtPresent;
            this.currentStateDescription = currentStateDescription;
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
