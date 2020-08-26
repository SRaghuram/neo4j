/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.neo4j.metrics.output.RotatingFileOutputStreamSupplier.RotationListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.adversaries.fs.AdversarialOutputStream;
import org.neo4j.function.Suppliers;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.output.RotatingFileOutputStreamSupplier.getAllArchives;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@EphemeralTestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class RotatingFileOutputStreamSupplierTest
{
    private static final Function<OutputStream, PrintWriter> OUTPUT_STREAM_CONVERTER =
            outputStream -> new PrintWriter( new OutputStreamWriter( outputStream, StandardCharsets.UTF_8 ) );
    private static final long TEST_TIMEOUT_MILLIS = 10_000;
    private static final java.util.concurrent.Executor DIRECT_EXECUTOR = Runnable::run;
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private SuppressOutput suppressOutput;

    private Path logFile;
    private Path archiveLogFile1;
    private Path archiveLogFile2;
    private Path archiveLogFile3;
    private Path archiveLogFile4;
    private Path archiveLogFile5;
    private Path archiveLogFile6;
    private Path archiveLogFile7;
    private Path archiveLogFile8;
    private Path archiveLogFile9;

    @BeforeEach
    void setup()
    {
        Path logDir = testDirectory.homePath();
        logFile = logDir.resolve( "logfile.log" );
        archiveLogFile1 = logDir.resolve( "logfile.log.1" );
        archiveLogFile2 = logDir.resolve( "logfile.log.2" );
        archiveLogFile3 = logDir.resolve( "logfile.log.3" );
        archiveLogFile4 = logDir.resolve( "logfile.log.4" );
        archiveLogFile5 = logDir.resolve( "logfile.log.5" );
        archiveLogFile6 = logDir.resolve( "logfile.log.6" );
        archiveLogFile7 = logDir.resolve( "logfile.log.7" );
        archiveLogFile8 = logDir.resolve( "logfile.log.8" );
        archiveLogFile9 = logDir.resolve( "logfile.log.9" );
    }

    @Test
    void createsLogOnConstruction() throws Exception
    {
        new RotatingFileOutputStreamSupplier( fileSystem, logFile, 250000, 0, 10, DIRECT_EXECUTOR );
        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
    }

    @Test
    void rotatesLogWhenSizeExceeded() throws Exception
    {
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0,
                10, DIRECT_EXECUTOR );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( false );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile2 ) ).isEqualTo( false );

        write( supplier, "Short" );
        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile2 ) ).isEqualTo( true );
    }

    @Test
    void limitsNumberOfArchivedLogs() throws Exception
    {
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 2,
                DIRECT_EXECUTOR );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( false );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile2 ) ).isEqualTo( false );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile2 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile3 ) ).isEqualTo( false );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile2 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile3 ) ).isEqualTo( false );
    }

    @Test
    void rotationShouldNotDeadlockOnListener() throws Exception
    {
        String logContent = "Output file created";
        final AtomicReference<Exception> listenerException = new AtomicReference<>( null );
        CountDownLatch latch = new CountDownLatch( 1 );
        RotationListener listener = new RotationListener()
        {
            @Override
            public void outputFileCreated( OutputStream out )
            {
                try
                {
                    Thread thread = new Thread( () ->
                    {
                        try
                        {
                            out.write( logContent.getBytes() );
                            out.flush();
                        }
                        catch ( IOException e )
                        {
                            listenerException.set( e );
                        }
                    } );
                    thread.start();
                    thread.join();
                }
                catch ( Exception e )
                {
                    listenerException.set( e );
                }
                super.outputFileCreated( out );
            }

            @Override
            public void rotationCompleted( OutputStream out )
            {
                latch.countDown();
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DefaultFileSystemAbstraction defaultFileSystemAbstraction = new DefaultFileSystemAbstraction();
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( defaultFileSystemAbstraction, logFile, 0, 0, 10, executor, listener );

        OutputStream outputStream = supplier.get();
        LockingPrintWriter lockingPrintWriter = new LockingPrintWriter( outputStream );
        lockingPrintWriter.withLock( () ->
        {
            supplier.rotate();
            latch.await();
            return Void.TYPE;
        } );

        shutDownExecutor( executor );

        String actual = Files.readString( logFile );
        assertEquals( logContent, actual );
        assertNull( listenerException.get() );
    }

    private void shutDownExecutor( ExecutorService executor ) throws InterruptedException
    {
        executor.shutdown();
        boolean terminated = executor.awaitTermination( TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS );
        if ( !terminated )
        {
            throw new IllegalStateException( "Rotation execution failed to complete within reasonable time." );
        }
    }

    @Test
    void shouldNotRotateLogWhenSizeExceededButNotDelay() throws Exception
    {
        UpdatableLongSupplier clock = new UpdatableLongSupplier( System.currentTimeMillis() );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( clock, fileSystem, logFile,
                10, SECONDS.toMillis( 60 ), 10, DIRECT_EXECUTOR, new RotationListener() );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( false );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile2 ) ).isEqualTo( false );

        write( supplier, "A string longer than 10 bytes" );

        clock.setValue( clock.getAsLong() + SECONDS.toMillis( 59 ) );
        write( supplier, "A string longer than 10 bytes" );

        clock.setValue( clock.getAsLong() + SECONDS.toMillis( 1 ) );
        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile2 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile3 ) ).isEqualTo( false );
    }

    @Test
    void shouldFindAllArchives() throws Exception
    {
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 2,
                DIRECT_EXECUTOR );

        write( supplier, "A string longer than 10 bytes" );
        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile2 ) ).isEqualTo( false );

        List<Path> allArchives = getAllArchives( fileSystem, logFile );
        assertThat( allArchives.size() ).isEqualTo( 1 );
        assertThat( allArchives ).contains( archiveLogFile1 );
    }

    @Test
    void shouldNotifyListenerWhenNewLogIsCreated() throws Exception
    {
        final CountDownLatch allowRotationComplete = new CountDownLatch( 1 );
        final CountDownLatch rotationComplete = new CountDownLatch( 1 );
        String outputFileCreatedMessage = "Output file created";
        String rotationCompleteMessage = "Rotation complete";

        RotationListener rotationListener = spy( new RotationListener()
        {
            @Override
            public void outputFileCreated( OutputStream out )
            {
                try
                {
                    allowRotationComplete.await( 1L, TimeUnit.SECONDS );
                    out.write( outputFileCreatedMessage.getBytes() );
                }
                catch ( InterruptedException | IOException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void rotationCompleted( OutputStream out )
            {
                rotationComplete.countDown();
                try
                {
                    out.write( rotationCompleteMessage.getBytes() );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );

        ExecutorService rotationExecutor = Executors.newSingleThreadExecutor();
        try
        {
            RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0,
                    10, rotationExecutor, rotationListener );

            write( supplier, "A string longer than 10 bytes" );
            write( supplier, "A string longer than 10 bytes" );

            allowRotationComplete.countDown();
            rotationComplete.await( 1L, TimeUnit.SECONDS );

            verify( rotationListener ).outputFileCreated( any( OutputStream.class ) );
            verify( rotationListener ).rotationCompleted( any( OutputStream.class ) );
        }
        finally
        {
            shutDownExecutor( rotationExecutor );
        }
    }

    @Test
    void shouldNotifyListenerOnRotationErrorDuringJobExecution() throws Exception
    {
        RotationListener rotationListener = mock( RotationListener.class );
        Executor executor = mock( Executor.class );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0,
                10, executor, rotationListener );
        OutputStream outputStream = supplier.get();

        RejectedExecutionException exception = new RejectedExecutionException( "text exception" );
        doThrow( exception ).when( executor ).execute( any( Runnable.class ) );

        write( supplier, "A string longer than 10 bytes" );
        assertThat( supplier.get() ).isEqualTo( outputStream );

        verify( rotationListener ).rotationError( exception, outputStream );
    }

    @Test
    void shouldReattemptRotationAfterExceptionDuringJobExecution() throws Exception
    {
        RotationListener rotationListener = mock( RotationListener.class );
        Executor executor = mock( Executor.class );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0,
                10, executor, rotationListener );
        OutputStream outputStream = supplier.get();

        RejectedExecutionException exception = new RejectedExecutionException( "text exception" );
        doThrow( exception ).when( executor ).execute( any( Runnable.class ) );

        write( supplier, "A string longer than 10 bytes" );
        assertThat( supplier.get() ).isEqualTo( outputStream );
        assertThat( supplier.get() ).isEqualTo( outputStream );

        verify( rotationListener, times( 2 ) ).rotationError( exception, outputStream );
    }

    @Test
    void shouldNotifyListenerOnRotationErrorDuringRotationIO() throws Exception
    {
        RotationListener rotationListener = mock( RotationListener.class );
        FileSystemAbstraction fs = spy( fileSystem );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fs, logFile, 10, 0, 10,
                DIRECT_EXECUTOR, rotationListener );
        OutputStream outputStream = supplier.get();

        IOException exception = new IOException( "text exception" );
        doThrow( exception ).when( fs ).renameFile( any( Path.class ), any( Path.class ) );

        write( supplier, "A string longer than 10 bytes" );
        assertThat( supplier.get() ).isEqualTo( outputStream );

        verify( rotationListener ).rotationError( eq( exception ), any( OutputStream.class ) );
    }

    @Test
    void shouldNotUpdateOutputStreamWhenClosedDuringRotation() throws Exception
    {
        final CountDownLatch allowRotationComplete = new CountDownLatch( 1 );

        RotationListener rotationListener = spy( new RotationListener()
        {
            @Override
            public void outputFileCreated( OutputStream out )
            {
                try
                {
                    allowRotationComplete.await();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );

        final List<OutputStream> mockStreams = new ArrayList<>();
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public OutputStream openAsOutputStream( Path fileName, boolean append ) throws IOException
            {
                final OutputStream stream = spy( super.openAsOutputStream( fileName, append ) );
                mockStreams.add( stream );
                return stream;
            }
        };

        ExecutorService rotationExecutor = Executors.newSingleThreadExecutor();
        try
        {
            RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fs, logFile, 10, 0,
                    10, rotationExecutor, rotationListener );
            OutputStream outputStream = supplier.get();

            write( supplier, "A string longer than 10 bytes" );
            assertThat( supplier.get() ).isEqualTo( outputStream );

            allowRotationComplete.countDown();
            supplier.close();
        }
        finally
        {
            shutDownExecutor( rotationExecutor );
        }

        assertStreamClosed( mockStreams.get( 0 ) );
    }

    @Test
    void shouldCloseAllOutputStreams() throws Exception
    {
        final List<OutputStream> mockStreams = new ArrayList<>();
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public OutputStream openAsOutputStream( Path fileName, boolean append ) throws IOException
            {
                final OutputStream stream = spy( super.openAsOutputStream( fileName, append ) );
                mockStreams.add( stream );
                return stream;
            }
        };

        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fs, logFile, 10, 0,
                10, DIRECT_EXECUTOR );

        write( supplier, "A string longer than 10 bytes" );

        supplier.close();

        assertStreamClosed( mockStreams.get( 0 ) );
    }

    @Test
    void shouldCloseAllStreamsDespiteError() throws Exception
    {
        final List<OutputStream> mockStreams = new ArrayList<>();
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public OutputStream openAsOutputStream( Path fileName, boolean append ) throws IOException
            {
                final OutputStream stream = spy( super.openAsOutputStream( fileName, append ) );
                mockStreams.add( stream );
                return stream;
            }
        };

        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fs, logFile, 10, 0, 10,
                DIRECT_EXECUTOR );

        write( supplier, "A string longer than 10 bytes" );
        write( supplier, "A string longer than 10 bytes" );

        IOException exception = new IOException( "test exception" );
        OutputStream mockStream = mockStreams.get( 1 );
        doThrow( exception ).when( mockStream ).close();

        IOException ioException = assertThrows( IOException.class, supplier::close );
        assertThat( ioException ).isSameAs( exception );
        verify( mockStream ).close();
    }

    @Test
    void shouldSurviveFilesystemErrors() throws Exception
    {
        final RandomAdversary adversary = new RandomAdversary( 0.1, 0.1, 0 );
        adversary.setProbabilityFactor( 0 );

        AdversarialFileSystemAbstraction adversarialFileSystem = new SensibleAdversarialFileSystemAbstraction(
                adversary, fileSystem );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( adversarialFileSystem,
                logFile, 1000, 0, 9, DIRECT_EXECUTOR );

        adversary.setProbabilityFactor( 1 );
        writeLines( supplier, 10000 );

        // run cleanly for a while, to allow it to fill any gaps left in log archive numbers
        adversary.setProbabilityFactor( 0 );
        writeLines( supplier, 10000 );

        assertThat( fileSystem.fileExists( logFile ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile1 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile2 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile3 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile4 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile5 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile6 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile7 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile8 ) ).isEqualTo( true );
        assertThat( fileSystem.fileExists( archiveLogFile9 ) ).isEqualTo( true );
    }

    private void write( RotatingFileOutputStreamSupplier supplier, String line )
    {
        PrintWriter writer = new PrintWriter( supplier.get() );
        writer.println( line );
        writer.flush();
    }

    private void writeLines( Supplier<OutputStream> outputStreamSupplier, int count )
    {
        Supplier<PrintWriter> printWriterSupplier = Suppliers.adapted( outputStreamSupplier, OUTPUT_STREAM_CONVERTER );
        for ( ; count >= 0; --count )
        {
            printWriterSupplier.get().println(
                    "We are what we repeatedly do. Excellence, then, is not an act, but a habit." );
            Thread.yield();
        }
    }

    private void assertStreamClosed( OutputStream stream )
    {
        assertThrows( ClosedChannelException.class, () -> stream.write( 0 ) );
    }

    private static class LockingPrintWriter extends PrintWriter
    {
        LockingPrintWriter( OutputStream out )
        {
            super( out );
            lock = new Object();

        }

        void withLock( Callable callable ) throws Exception
        {
            synchronized ( lock )
            {
                callable.call();
            }
        }
    }

    private static class UpdatableLongSupplier implements LongSupplier
    {
        private final AtomicLong longValue;

        UpdatableLongSupplier( long value )
        {
            this.longValue = new AtomicLong( value );
        }

        long setValue( long value )
        {
            return longValue.getAndSet( value );
        }

        @Override
        public long getAsLong()
        {
            return longValue.get();
        }
    }

    private static class SensibleAdversarialFileSystemAbstraction extends AdversarialFileSystemAbstraction
    {
        private final Adversary adversary;
        private final FileSystemAbstraction delegate;

        SensibleAdversarialFileSystemAbstraction( Adversary adversary, FileSystemAbstraction delegate )
        {
            super( adversary, delegate );
            this.adversary = adversary;
            this.delegate = delegate;
        }

        @Override
        public OutputStream openAsOutputStream( Path fileName, boolean append ) throws IOException
        {
            // Default adversarial might throw a java.lang.SecurityException here, which is an exception
            // that should not be survived
            adversary.injectFailure( NoSuchFileException.class );
            final OutputStream outputStream = delegate.openAsOutputStream( fileName, append );
            return new AdversarialOutputStream( outputStream, adversary )
            {
                @Override
                public void write( byte[] b ) throws IOException
                {
                    // Default adversarial might throw a NullPointerException.class or IndexOutOfBoundsException here,
                    // which are exceptions that should not be survived
                    adversary.injectFailure( IOException.class );
                    outputStream.write( b );
                }

                @Override
                public void write( byte[] b, int off, int len ) throws IOException
                {
                    // Default adversarial might throw a NullPointerException.class or IndexOutOfBoundsException here,
                    // which are exceptions that should not be survived
                    adversary.injectFailure( IOException.class );
                    outputStream.write( b, off, len );
                }
            };
        }

        @Override
        public boolean fileExists( Path file )
        {
            // Default adversarial might throw a java.lang.SecurityException here, which is an exception
            // that should not be survived
            return delegate.fileExists( file );
        }

        @Override
        public long getFileSize( Path fileName )
        {
            // Default adversarial might throw a java.lang.SecurityException here, which is an exception
            // that should not be survived
            return delegate.getFileSize( fileName );
        }
    }
}
