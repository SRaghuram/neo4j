#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#


<#
.SYNOPSIS
Reconfigure a Neo4j Server Windows Service

.DESCRIPTION
Reconfigure a Neo4j Server Windows Service

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.EXAMPLE
Reconfigure-Neo4jServer -Neo4jServer $ServerObject

Reconfigure the Neo4j Windows Service for the Neo4j installation at $ServerObject

.OUTPUTS
System.Int32
0 = Service is successfully reconfigured
non-zero = an error occured

.NOTES
This function is private to the powershell module

#>
Function Reconfigure-Neo4jServer
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Medium')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$true)]
    [PSCustomObject]$Neo4jServer
  )

  Begin
  {
  }

  Process
  {
    $Name = Get-Neo4jWindowsServiceName -Neo4jServer $Neo4jServer -ErrorAction Stop

    $result = Get-Service -Name $Name -ComputerName '.' -ErrorAction 'SilentlyContinue'
    if ($result -ne $null)
    {
      $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $Neo4jServer -ForServerReconfigure
      if ($prunsrv -eq $null) { throw "Could not determine the command line for PRUNSRV" }

      Write-Verbose "Reconfiguring Neo4j as a service with command line $($prunsrv.cmd) $($prunsrv.args)"
      $stdError = New-Neo4jTempFile -Prefix 'stderr'
      Write-Verbose $prunsrv
      $result = (Start-Process -FilePath $prunsrv.cmd -ArgumentList $prunsrv.args -Wait -NoNewWindow -PassThru -WorkingDirectory $Neo4jServer.Home -RedirectStandardError $stdError)
      Write-Verbose "Returned exit code $($result.ExitCode)"

      # Process the output
      if ($result.ExitCode -eq 0) {
        Write-Host "Neo4j service reconfigured"
      } else {
        Write-Host "Neo4j service did not reconfigure"
        # Write out STDERR if it did not reconfigure
        Get-Content -Path $stdError -ErrorAction 'SilentlyContinue' | ForEach-Object -Process {
          Write-Host $_
        }
      }

      # Remove the temp file
      If (Test-Path -Path $stdError) { Remove-Item -Path $stdError -Force | Out-Null }

      Write-Output $result.ExitCode
    } else {
      Write-Verbose "Service reconfigure failed"
      Write-Output 1
    }
  }

  End
  {
  }
}

