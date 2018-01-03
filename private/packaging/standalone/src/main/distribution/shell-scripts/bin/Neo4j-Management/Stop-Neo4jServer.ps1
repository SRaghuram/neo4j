#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#


<#
.SYNOPSIS
Stop a Neo4j Server Windows Service

.DESCRIPTION
Stop a Neo4j Server Windows Service

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.EXAMPLE
Stop-Neo4jServer -Neo4jServer $ServerObject

Stop the Neo4j Windows Windows Service for the Neo4j installation at $ServerObject

.OUTPUTS
System.Int32
0 = Service was stopped and not running
non-zero = an error occured

.NOTES
This function is private to the powershell module

#>
Function Stop-Neo4jServer
{
  [cmdletBinding(SupportsShouldProcess=$true,ConfirmImpact='Medium')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$true)]
    [PSCustomObject]$Neo4jServer

  )
  
  Begin
  {
  }

  Process
  {
    $ServiceName = Get-Neo4jWindowsServiceName -Neo4jServer $Neo4jServer -ErrorAction Stop

    Write-Verbose "Stopping the service.  This can take some time..."
    $result = Stop-Service -Name $ServiceName -PassThru -ErrorAction Stop
    
    if ($result.Status -eq 'Stopped') {
      Write-Host "Neo4j windows service stopped"
      return 0
    }
    else {
      Write-Host "Neo4j windows was sent the Stop command but is currently $($result.Status)"
      return 2
    }
  }
  
  End
  {
  }
}
