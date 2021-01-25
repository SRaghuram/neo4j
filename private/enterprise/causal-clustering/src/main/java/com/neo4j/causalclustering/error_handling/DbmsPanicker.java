/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.configuration.EnterpriseEditionSettings;

import org.neo4j.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

public interface DbmsPanicker
{

    void panic( DbmsPanicEvent panic );

    static DbmsPanicker buildFor( Config config, LogService logService )
    {
        return config.get( EnterpriseEditionSettings.shutdown_on_dbms_panic ) ? new SystemExitingDbmsPanicker( logService )
                                                                              : new JustLogErrorsDbmsPanicker( logService );
    }

    abstract class AbstractLoggingDbmsPanicker implements DbmsPanicker
    {
        protected final Log userLog;
        protected final Log debugLog;

        private AbstractLoggingDbmsPanicker( LogService logService )
        {
            this.userLog = logService.getUserLog( this.getClass() );
            this.debugLog = logService.getInternalLog( this.getClass() );
        }
    }

    class SystemExitingDbmsPanicker extends AbstractLoggingDbmsPanicker
    {
        private SystemExitingDbmsPanicker( LogService logService )
        {
            super( logService );
        }

        @Override
        public void panic( DbmsPanicEvent panic )
        {
            debugLog.error( "DbMS Panic [%s]. Terminating neo4j process.", panic.getReason().getDescription(), panic.getCause() );
            userLog.error( "The Neo4j Database Management System has panicked. The process will now shut down." );
            System.exit( panic.getExitCode() );
        }
    }

    class JustLogErrorsDbmsPanicker extends AbstractLoggingDbmsPanicker
    {
        private JustLogErrorsDbmsPanicker( LogService logService )
        {
            super( logService );
        }

        @Override
        public void panic( DbmsPanicEvent panic )
        {
            debugLog.error( "DbMS Panic [%s].", panic.getReason().getDescription(), panic.getCause() );
            userLog.error( "The Neo4j Database Management System has panicked. " +
                           "The process will continue to run however to recover full functionality you should restart Neo4j" );
        }
    }
}
