/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.admin;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import javax.annotation.Nonnull;

import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.helpers.collection.Iterables;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.commandline.Util.neo4jVersion;
import static org.neo4j.commandline.admin.AdminTool.STATUS_ERROR;
import static org.neo4j.commandline.admin.AdminTool.STATUS_SUCCESS;

class AdminToolTest
{
    @Test
    void shouldExecuteTheCommand() throws CommandFailed, IncorrectUsage
    {
        AdminCommand command = mock( AdminCommand.class );
        new AdminTool( cannedCommand( "command", command ), new NullOutsideWorld(), false )
                .execute( null, null, "command", "the", "other", "args" );
        verify( command ).execute( new String[]{"the", "other", "args"} );
    }

    @Test
    void shouldExit0WhenEverythingWorks()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        new AdminTool( new CannedLocator( new NullCommandProvider() ), outsideWorld, false )
                .execute( null, null, "null" );
        verify( outsideWorld ).exit( STATUS_SUCCESS );
    }

    @Test
    void shouldAddTheHelpCommandToThoseProvidedByTheLocator()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        new AdminTool( new NullCommandLocator(), outsideWorld, false )
                .execute( null, null, "help" );
        verify( outsideWorld ).stdOutLine( "    help" );
    }

    @Test
    void shouldProvideFeedbackWhenNoCommandIsProvided()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        new AdminTool( new NullCommandLocator(), outsideWorld, false ).execute( null, null );
        verify( outsideWorld ).stdErrLine( "you must provide a command" );
        verify( outsideWorld ).stdErrLine( "usage: neo4j-admin <command>" );
        verify( outsideWorld ).exit( STATUS_ERROR );
    }

    @Test
    void shouldProvideFeedbackIfTheCommandThrowsARuntimeException()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        AdminCommand command = args ->
        {
            throw new RuntimeException( "the-exception-message" );
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld ).stdErrLine( "unexpected error: the-exception-message" );
        verify( outsideWorld ).exit( STATUS_ERROR );
    }

    @Test
    void shouldPrintTheStacktraceWhenTheCommandThrowsARuntimeExceptionIfTheDebugFlagIsSet()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        RuntimeException exception = new RuntimeException( "" );
        AdminCommand command = args ->
        {
            throw exception;
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, true )
                .execute( null, null, "exception" );
        verify( outsideWorld ).printStacktrace( exception );
    }

    @Test
    void shouldNotPrintTheStacktraceWhenTheCommandThrowsARuntimeExceptionIfTheDebugFlagIsNotSet()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        RuntimeException exception = new RuntimeException( "" );
        AdminCommand command = args ->
        {
            throw exception;
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld, never() ).printStacktrace( exception );
    }

    @Test
    void shouldProvideFeedbackIfTheCommandFails()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        AdminCommand command = args ->
        {
            throw new CommandFailed( "the-failure-message" );
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld ).stdErrLine( "command failed: the-failure-message" );
        verify( outsideWorld ).exit( STATUS_ERROR );
    }

    @Test
    void shouldPrintTheStacktraceWhenTheCommandFailsIfTheDebugFlagIsSet()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CommandFailed exception = new CommandFailed( "" );
        AdminCommand command = args ->
        {
            throw exception;
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, true )
                .execute( null, null, "exception" );
        verify( outsideWorld ).printStacktrace( exception );
    }

    @Test
    void shouldNotPrintTheStacktraceWhenTheCommandFailsIfTheDebugFlagIsNotSet()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CommandFailed exception = new CommandFailed( "" );
        AdminCommand command = args ->
        {
            throw exception;
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld, never() ).printStacktrace( exception );
    }

    @Test
    void shouldProvideFeedbackIfTheCommandReportsAUsageProblem()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        AdminCommand command = args ->
        {
            throw new IncorrectUsage( "the-usage-message" );
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, false )
                .execute( null, null, "exception" );
        InOrder inOrder = inOrder( outsideWorld );
        inOrder.verify( outsideWorld ).stdErrLine( "the-usage-message" );
        verify( outsideWorld ).exit( STATUS_ERROR );
    }

    @Test
    void helpArgumentPrintsHelp()
    {
        AdminCommand command = mock( AdminCommand.class );
        OutsideWorld outsideWorld = mock( OutsideWorld.class );

        new AdminTool( cannedCommand( "command", command ), outsideWorld, false )
                .execute( null, null, "--help" );

        verifyNoMoreInteractions( command );
        verify( outsideWorld ).stdErrLine( "unrecognized command: --help" );
        verify( outsideWorld ).stdErrLine( "usage: neo4j-admin <command>" );
        verify( outsideWorld ).exit( STATUS_ERROR );
    }

    @Test
    void helpArgumentPrintsHelpForCommand()
    {
        AdminCommand command = mock( AdminCommand.class );
        OutsideWorld outsideWorld = mock( OutsideWorld.class );

        new AdminTool( cannedCommand( "command", command ), outsideWorld, false )
                .execute( null, null, "command", "--help" );

        verifyNoMoreInteractions( command );
        verify( outsideWorld ).stdErrLine( "unknown argument: --help" );
        verify( outsideWorld ).stdErrLine( "usage: neo4j-admin command " );
        verify( outsideWorld ).exit( STATUS_ERROR );
    }

    @Test
    void versionArgumentPrintsVersion()
    {
        AdminCommand command = mock( AdminCommand.class );
        OutsideWorld outsideWorld = mock( OutsideWorld.class );

        new AdminTool( cannedCommand( "command", command ), outsideWorld, false )
                .execute( null, null, "--version" );

        verifyNoMoreInteractions( command );
        verify( outsideWorld ).stdOutLine( "neo4j-admin " + neo4jVersion() );
        verify( outsideWorld ).exit( STATUS_SUCCESS );
    }

    @Test
    void versionArgumentPrintsVersionEvenWithCommand()
    {
        AdminCommand command = mock( AdminCommand.class );
        OutsideWorld outsideWorld = mock( OutsideWorld.class );

        new AdminTool( cannedCommand( "command", command ), outsideWorld, false )
                .execute( null, null, "command", "--version" );

        verifyNoMoreInteractions( command );
        verify( outsideWorld ).stdOutLine( "neo4j-admin " + neo4jVersion() );
        verify( outsideWorld ).exit( STATUS_SUCCESS );
    }

    private static CannedLocator cannedCommand( final String name, AdminCommand command )
    {
        return new CannedLocator( new AdminCommand.Provider( name )
        {
            @Override
            @Nonnull
            public Arguments allArguments()
            {
                return Arguments.NO_ARGS;
            }

            @Override
            @Nonnull
            public String description()
            {
                return "";
            }

            @Override
            @Nonnull
            public String summary()
            {
                return "";
            }

            @Override
            @Nonnull
            public AdminCommandSection commandSection()
            {
                return AdminCommandSection.general();
            }

            @Override
            @Nonnull
            public AdminCommand create( CommandContext ctx )
            {
                return command;
            }
        } );
    }

    private static class NullCommandLocator implements CommandLocator
    {
        @Override
        public AdminCommand.Provider findProvider( String s )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public Iterable<AdminCommand.Provider> getAllProviders()
        {
            return Iterables.empty();
        }
    }

    private static class NullCommandProvider extends AdminCommand.Provider
    {
        NullCommandProvider()
        {
            super( "null" );
        }

        @Override
        public Arguments allArguments()
        {
            return Arguments.NO_ARGS;
        }

        @Override
        public String description()
        {
            return "";
        }

        @Override
        public String summary()
        {
            return "";
        }

        @Override
        public AdminCommandSection commandSection()
        {
            return AdminCommandSection.general();
        }

        @Override
        public AdminCommand create( CommandContext ctx )
        {
            return args -> { };
        }
    }
}
