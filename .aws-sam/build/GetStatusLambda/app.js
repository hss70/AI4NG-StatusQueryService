const { DynamoDBClient, QueryCommand, ScanCommand } = require("@aws-sdk/client-dynamodb");
const { CloudWatchLogsClient, FilterLogEventsCommand } = require("@aws-sdk/client-cloudwatch-logs");
const { unmarshall } = require("@aws-sdk/util-dynamodb");
const classifierLogGroup = process.env.CLASSIFIER_LOG_GROUP;

const ddbClient = new DynamoDBClient();
const logsClient = new CloudWatchLogsClient();
const tableName = process.env.STATUS_TABLE;

exports.handler = async (event) => {
    try {
        const path = event.routeKey;
        const queryParams = event.queryStringParameters || {};

        // Route to appropriate handler
        if (path === 'GET /api/status') {
            return await getAllSessions(queryParams);
        } else if (path === 'GET /api/status/{sessionId}') {
            const sessionId = parseInt(event.pathParameters.sessionId);
            return await getSessionById(sessionId);
        }

        return {
            statusCode: 404,
            body: JSON.stringify({ error: 'Route not found' })
        };

    } catch (error) {
        console.error('Error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: error.message })
        };
    }
};

async function getSessionById(sessionId) {
    const params = {
        TableName: tableName,
        IndexName: 'SessionIdIndex',
        KeyConditionExpression: 'sessionId = :sessionId',
        ExpressionAttributeValues: {
            ':sessionId': { N: sessionId.toString() }
        },
        Limit: 1
    };

    const response = await ddbClient.send(new QueryCommand(params));

    if (!response.Items || response.Items.length === 0) {
        return {
            statusCode: 404,
            body: JSON.stringify({ error: 'Session not found' })
        };
    }

    const item = unmarshall(response.Items[0]);
    const statusInfo = formatStatusItem(item, sessionId);

    // Add ECS logs if available
    if (statusInfo.status === 'PROCESSING' || statusInfo.status === 'FAILED') {
        statusInfo.logs = await getECSLogs(sessionId, {
            maxLogs: 50,
            lookbackMs: 24 * 60 * 60 * 1000, // 24 hours
            initialLookbackMs: 5 * 60 * 1000,   // start from 5 mins 
            excludePhrases
        });
    }

    return {
        statusCode: 200,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(statusInfo)
    };
}

async function getAllSessions(queryParams) {
    const status = queryParams.status;
    let params;

    if (status) {
        // Filter by status
        params = {
            TableName: tableName,
            FilterExpression: '#status = :status',
            ExpressionAttributeNames: {
                '#status': 'status'
            },
            ExpressionAttributeValues: {
                ':status': { S: status }
            }
        };
    } else {
        // Get all sessions
        params = {
            TableName: tableName
        };
    }

    const response = await ddbClient.send(new ScanCommand(params));

    const sessions = response.Items.map(item => {
        const unmarshalled = unmarshall(item);
        return formatStatusItem(unmarshalled, unmarshalled.sessionId);
    });

    return {
        statusCode: 200,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessions, count: sessions.length })
    };
}

function formatStatusItem(item, sessionId) {
    // Parse startTime - could be ISO string or Unix timestamp
    let startTimeUnix = 0;
    let startTimeISO = '';

    if (item.startTime) {
        if (typeof item.startTime === 'string' && item.startTime.includes('T')) {
            // ISO string format like "2025-08-20T12:48:20.387Z"
            const date = new Date(item.startTime);
            startTimeUnix = Math.floor(date.getTime() / 1000);
            startTimeISO = item.startTime;
        } else {
            // Unix timestamp or number
            startTimeUnix = parseInt(item.startTime);
            startTimeISO = new Date(startTimeUnix * 1000).toISOString();
        }
    }

    const statusInfo = {
        sessionName: item.sessionName || '',
        sessionId: sessionId || item.sessionId || 0,
        status: item.status || 'UNKNOWN',
        userId: item.userId || '',
        startTime: startTimeUnix,
        processingDuration: parseInt(item.processingDuration) || 0,
        resultsPath: item.resultsPath || '',
        exitCode: parseInt(item.exitCode) || -1,
        startTimeISO: startTimeISO
    };

    return statusInfo;
}

const excludePhrases = [
    // MATLAB/env boilerplate
    "Prep plus (EEG)",
    "Running: Finished",
    "Set env:",
    "LD_LIBRARY_PATH",
    "________________________",
    "------------------------------------------",
    "---",
    "ans =",
    "downsample is off",
    "dataDir =",
    "chanlocDir =",
    "dir0 =",
    "SubjID2:",
    "BandID:",

    // repetitive loads/saves noise
    "Dataset loading: DONE",
    "Loading A10_offlineClass_prep_01",
    "Loading A09_EEG_validation_01",
    "Loading EEG_rec dataset",
    "Loading tr{",                 // e.g. Loading tr{1,1}.mat ...
    "At /app/work/Work/TrainTest", // path echo
    "Saving config structure",
    "Saving autorun structure",
    "Saving EEG_validation structure",
    "Saving tr_validMask structure",
    "Saving classTrials",
    "Saving result structure",
    "Saving va_trans",
    "Saving t1_results",
    "Saving t1_result_table",
    "Saving online structure",
    "Autosave (it may take some minutes)",

    // plot/figure chatter
    "Saved JSON config:",
    "CSP-MI topoplot",
    "DA plot",
    "heatmap (Freq v4)",
    "Warning:",
    "In topoplot",
    "VAv2_Hacked_VB_FBCSP_eval_figures_func_01",
    "TAv2_TrainTest",
    "T1_proper",
    "FBCSP_Training",

    // S3 transfer/manifest spam
    "Adding to manifest:",
    "upload: ",                    // S3 sync lines
    "Completed ",                  // progress counters
    "download: s3://"
];


function escapeRegExp(s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function getECSLogs(
    sessionId,
    {
        maxLogs = 50,
        maxLookbackMs = 24 * 60 * 60 * 1000, // hard cap
        initialLookbackMs = 5 * 60 * 1000,   // start small
        excludePhrases = []
    } = {}
) {
    const negatives = excludePhrases
        .filter(Boolean)
        .map(p => `-"${p.replace(/"/g, '\\"')}"`)
        .join(" ");
    const baseFilter = `"[SESSION_ID=${sessionId}]" ${negatives}`.trim();

    const excludeRegexes = [
        /^\s*\[SESSION_ID=\d+\]\s*$/i,
        ...excludePhrases.map(s => new RegExp(escapeRegExp(s), "i")),
    ];
    const isNoise = m => excludeRegexes.some(rx => rx.test(m || ""));

    let lookback = initialLookbackMs;
    let collected = [];

    while (true) {
        const startTime = Date.now() - Math.min(lookback, maxLookbackMs);

        let nextToken;
        const seen = new Set();

        do {
            const resp = await logsClient.send(new FilterLogEventsCommand({
                logGroupName: classifierLogGroup,
                filterPattern: baseFilter,
                startTime,
                endTime: Date.now(),
                interleaved: true,
                limit: 1000,
                nextToken,
            }));

            if (resp?.events?.length) {
                for (const e of resp.events) {
                    if (!seen.has(e.eventId) && !isNoise(e.message)) {
                        seen.add(e.eventId);
                        collected.push(e);
                    }
                }
            }
            nextToken = resp.nextToken;
        } while (nextToken);

        // If we’ve got enough (in this window), stop; otherwise expand window.
        if (collected.length >= maxLogs || lookback >= maxLookbackMs) break;

        // expand window and try again
        lookback = Math.min(lookback * 2, maxLookbackMs);
        collected = []; // reset so we don’t double-count across different windows
    }

    collected.sort((a, b) => a.timestamp - b.timestamp);
    return collected.slice(-maxLogs).reverse().map(e => ({
        timestamp: new Date(e.timestamp).toISOString(),
        message: (e.message || "").trim(),
    }));
}