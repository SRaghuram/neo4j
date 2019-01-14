/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.auth;

import com.neo4j.server.security.enterprise.CommercialSecurityModule;
import com.neo4j.server.security.enterprise.auth.InternalFlatFileRealm;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobSchedulerAdapter;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertTrue;

abstract class FlatFileStressBase
{
    private final long ONE_SECOND = 1000;
    protected long TIMEOUT_IN_SECONDS = 10;
    protected int N = 10;
    protected int ERROR_LIMIT = 100;

    InternalFlatFileRealm flatFileRealm;
    private UserRepository userRepository;
    private RoleRepository roleRepository;

    private volatile boolean keepRunning = true;
    final Set<Throwable> errors = ConcurrentHashMap.newKeySet();

    @Before
    public void setup() throws Throwable
    {
        Config config = Config.defaults();
        LogProvider logProvider = NullLogProvider.getInstance();
        JobScheduler jobScheduler = new JobSchedulerAdapter();

        userRepository = CommunitySecurityModule.getUserRepository( config, logProvider, getFileSystem() );
        roleRepository = CommercialSecurityModule.getRoleRepository( config, logProvider, getFileSystem() );

        flatFileRealm = new InternalFlatFileRealm(
                userRepository,
                roleRepository,
                new BasicPasswordPolicy(),
                new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() ),
                jobScheduler,
                CommunitySecurityModule.getInitialUserRepository( config, logProvider, getFileSystem() ),
                CommercialSecurityModule.getDefaultAdminRepository( config, logProvider, getFileSystem() )
            );

        flatFileRealm.init();
        flatFileRealm.start();
    }

    abstract FileSystemAbstraction getFileSystem();

    @After
    public void teardown() throws Throwable
    {
        flatFileRealm.stop();
        flatFileRealm.shutdown();
    }

    @Test
    public void shouldMaintainConsistency() throws InterruptedException, IOException
    {
        ExecutorService service = setupWorkload( N );

        for ( int t = 0; t < TIMEOUT_IN_SECONDS; t++ )
        {
            Thread.sleep( ONE_SECOND );
            if ( errors.size() > ERROR_LIMIT )
            {
                break;
            }
        }

        keepRunning = false;
        service.shutdown();
        service.awaitTermination( 5, SECONDS );

        // Assert that no errors occured
        String msg = String.join( System.lineSeparator(),
                errors.stream().map( Throwable::getMessage ).collect( Collectors.toList() ) );
        assertThat( msg, errors, empty() );

        // Assert that user and role repos are consistent
        ListSnapshot<User> users = userRepository.getPersistedSnapshot();
        ListSnapshot<RoleRecord> roles = roleRepository.getPersistedSnapshot();
        assertTrue(
                "User and role repositories are no longer consistent",
                RoleRepository.validate( users.values(), roles.values() )
            );
    }

    abstract ExecutorService setupWorkload( int n );

    abstract class IrrationalAdmin implements Runnable
    {
        final Random random = new Random();
        Runnable[] actions;

        @Override
        public void run()
        {
            while ( keepRunning )
            {
                randomAction().run();
            }
        }

        private Runnable randomAction()
        {
            return actions[ random.nextInt( actions.length ) ];
        }

        void setActions( Runnable... actions )
        {
            this.actions = actions;
        }
    }
}
