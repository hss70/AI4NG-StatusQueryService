getStatusLambda
Purpose: Provide API endpoints for processing status

Triggers:
- GET /api/status/{sessionId} - Get specific session
- GET /api/status - Get all sessions
- GET /api/status?status=COMPLETED - Filter by status

Inputs:
- sessionId path parameter (for specific session)
- status query parameter (optional filter)

Outputs:
- Single session object
- Array of sessions with count

Valid Status Filters:
- PROCESSING
- COMPLETED
- FAILED
- UNKNOWN

Permissions: dynamodb:Query, dynamodb:Scan on status table