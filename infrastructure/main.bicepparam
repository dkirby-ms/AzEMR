using 'main.bicep'

// Basic configuration
param namePrefix = 'openemr'
param environment = 'dev'


// MySQL configuration
param mysqlAdminLogin = 'openemradmin'
param mysqlAdminPassword = 'SecurePassword123!'

// Security and access
param allowPublicAccess = false
param enableTlsIngress = true
param enableZoneRedundancy = false

// Container configuration
param openEmrImage = 'openemr/openemr:latest'
