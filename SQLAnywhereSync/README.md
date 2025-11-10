# SQL Anywhere Sync Tool

## Setup Instructions

1. **Configure Database Connection**
   - Edit `config.json` file
   - Update the DSN, username, and password for your SQL Anywhere database
   - Update the API base URL to point to your web API

2. **Run the Sync Tool**
   - **Windows**: Double-click `sync.bat` or run `SyncTool.exe` directly
   - **Linux/Mac**: Run `./sync.sh` or `./SyncTool` directly

## Configuration

Edit the `config.json` file to match your environment:

```json
{
  "database": {
    "dsn": "YOUR_DATABASE_DSN",
    "username": "YOUR_USERNAME", 
    "password": "YOUR_PASSWORD"
  },
  "api": {
    "base_url": "https://your-api-domain.com/api",
    "upload_endpoint": "/upload-users/",
    "timeout": 30
  }
}
```

## Requirements

- SQL Anywhere database with ODBC driver configured
- Internet connection to reach the web API
- Windows: No additional software required
- Linux/Mac: Ensure execute permissions are set

## Troubleshooting

- Check the log files created in the same directory for detailed error information
- Verify ODBC DSN is properly configured in your system
- Ensure the API endpoint is accessible from your network
- Check that the SQL Anywhere database table 'acc_users' exists and is accessible

## Support

For support, check the log files for detailed error messages.
