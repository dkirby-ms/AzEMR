using 'main.bicep'

// Basic configuration
param namePrefix = 'openemr'
param environment = 'dev'


// MySQL configuration
param mysqlAdminLogin = 'openemradmin'
param mysqlAdminPassword = 'SecurePassword123!'

// Network configuration
param vnetCidr = '10.0.0.0/16'
param containerAppsSubnetCidr = '10.0.1.0/24'
param mysqlSubnetCidr = '10.0.2.0/24'

// Security and access
param allowPublicAccess = false
param enableTlsIngress = true
param enableZoneRedundancy = false

// Container configuration
param openEmrImage = 'openemr/openemr:latest'
