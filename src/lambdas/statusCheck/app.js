const {
    DynamoDBClient,
    QueryCommand,
    GetItemCommand
} = require("@aws-sdk/client-dynamodb");
const {
    CloudWatchLogsClient,
    FilterLogEventsCommand
} = require("@aws-sdk/client-cloudwatch-logs");
const { unmarshall, marshall } = require("@aws-sdk/util-dynamodb");

const ddbClient = new DynamoDBClient({});
const logsClient = new CloudWatchLogsClient({});

const tableName = process.env.STATUS_TABLE;
const classifierLogGroup = process.env.CLASSIFIER_LOG_GROUP;

exports.handler = async (event) => {
    try {
        const path = event.routeKey;
        const queryParams = event.queryStringParameters || {};

        if (path === "GET /api/status") {
            return await getAllSessions(queryParams);
        }

        if (path === "GET /api/status/{sessionId}") {
            const rawSessionId = event.pathParameters?.sessionId;
            const sessionId = parseInt(rawSessionId, 10);

            if (!Number.isFinite(sessionId) || sessionId <= 0) {
                return jsonResponse(400, { error: "Invalid sessionId" });
            }

            return await getSessionById(sessionId);
        }

        return jsonResponse(404, { error: "Route not found" });
    } catch (error) {
        console.error("Unhandled error:", error);
        return jsonResponse(500, { error: error.message || "Internal server error" });
    }
};

async function getSessionById(sessionId) {
    const params = {
        TableName: tableName,
        Key: {
            sessionId: { N: sessionId.toString() }
        }
    };

    const response = await ddbClient.send(new GetItemCommand(params));

    if (!response.Item) {
        return jsonResponse(404, { error: "Session not found" });
    }

    const item = unmarshall(response.Item);
    const statusInfo = formatStatusItem(item, sessionId);

    if (statusInfo.status === "PROCESSING" || statusInfo.status === "FAILED") {
        statusInfo.logs = await getECSLogs(sessionId, {
            maxLogs: 50,
            maxLookbackMs: 24 * 60 * 60 * 1000,
            initialLookbackMs: 5 * 60 * 1000,
            excludePhrases
        });
    }

    return jsonResponse(200, statusInfo);
}

async function getAllSessions(queryParams) {
    const status = queryParams.status?.trim();
    const limit = Math.min(Math.max(parseInt(queryParams.limit || "25", 10) || 25, 1), 100);

    let exclusiveStartKey;
    if (queryParams.nextToken) {
        try {
            exclusiveStartKey = JSON.parse(
                Buffer.from(queryParams.nextToken, "base64").toString("utf8")
            );
        } catch (err) {
            return jsonResponse(400, { error: "Invalid nextToken" });
        }
    }

    let params;

    if (status) {
        params = {
            TableName: tableName,
            IndexName: "StatusStartTimeIndex",
            KeyConditionExpression: "#status = :status",
            ExpressionAttributeNames: {
                "#status": "status"
            },
            ExpressionAttributeValues: {
                ":status": { S: status }
            },
            ScanIndexForward: false,
            Limit: limit
        };
    } else {
        params = {
            TableName: tableName,
            IndexName: "EntityTypeStartTimeIndex",
            KeyConditionExpression: "entityType = :entityType",
            ExpressionAttributeValues: {
                ":entityType": { S: "SESSION_STATUS" }
            },
            ScanIndexForward: false,
            Limit: limit
        };
    }

    if (exclusiveStartKey) {
        params.ExclusiveStartKey = exclusiveStartKey;
    }

    const response = await ddbClient.send(new QueryCommand(params));

    const sessions = (response.Items || []).map(item => {
        const unmarshalled = unmarshall(item);
        return formatStatusItem(unmarshalled, unmarshalled.sessionId);
    });

    const nextToken = response.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(response.LastEvaluatedKey), "utf8").toString("base64")
        : null;

    return jsonResponse(200, {
        sessions,
        count: sessions.length,
        nextToken
    });
}

function formatStatusItem(item, sessionId) {
    let startTimeUnix = 0;
    let startTimeISO = "";

    if (item.startTime) {
        if (typeof item.startTime === "string" && item.startTime.includes("T")) {
            const date = new Date(item.startTime);
            startTimeUnix = Math.floor(date.getTime() / 1000);
            startTimeISO = item.startTime;
        } else {
            startTimeUnix = parseInt(item.startTime, 10) || 0;
            startTimeISO = startTimeUnix > 0
                ? new Date(startTimeUnix * 1000).toISOString()
                : "";
        }
    }

    return {
        sessionName: item.sessionName || "",
        sessionId: sessionId || item.sessionId || 0,
        status: item.status || "UNKNOWN",
        userId: item.userId || "",
        startTime: startTimeUnix,
        processingDuration: parseInt(item.processingDuration, 10) || 0,
        resultsPath: item.resultsPath || "",
        exitCode: parseInt(item.exitCode, 10) || -1,
        startTimeISO
    };
}

const excludePhrases = [
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
    "Dataset loading: DONE",
    "Loading A10_offlineClass_prep_01",
    "Loading A09_EEG_validation_01",
    "Loading EEG_rec dataset",
    "Loading tr{",
    "At /app/work/Work/TrainTest",
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
    "Adding to manifest:",
    "upload: ",
    "Completed ",
    "download: s3://"
];

function escapeRegExp(s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function getECSLogs(
    sessionId,
    {
        maxLogs = 50,
        maxLookbackMs = 24 * 60 * 60 * 1000,
        initialLookbackMs = 5 * 60 * 1000,
        excludePhrases = []
    } = {}
) {
    if (!classifierLogGroup) {
        console.warn("CLASSIFIER_LOG_GROUP not set");
        return [];
    }

    const negatives = excludePhrases
        .filter(Boolean)
        .map(p => `-"${p.replace(/"/g, '\\"')}"`)
        .join(" ");

    const baseFilter = `"[SESSION_ID=${sessionId}]" ${negatives}`.trim();

    const excludeRegexes = [
        /^\s*\[SESSION_ID=\d+\]\s*$/i,
        ...excludePhrases.map(s => new RegExp(escapeRegExp(s), "i"))
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
                nextToken
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

        if (collected.length >= maxLogs || lookback >= maxLookbackMs) {
            break;
        }

        lookback = Math.min(lookback * 2, maxLookbackMs);
        collected = [];
    }

    collected.sort((a, b) => a.timestamp - b.timestamp);

    return collected.slice(-maxLogs).reverse().map(e => ({
        timestamp: new Date(e.timestamp).toISOString(),
        message: (e.message || "").trim()
    }));
}

function jsonResponse(statusCode, body) {
    return {
        statusCode,
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(body)
    };
}