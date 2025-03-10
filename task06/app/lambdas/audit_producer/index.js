const AWS = require("aws-sdk");

// Initialize DynamoDB DocumentClient
const dynamodb = new AWS.DynamoDB.DocumentClient();

// Environment variable for the Audit table name
const AUDIT_TABLE_NAME = process.env.AUDIT_TABLE;

exports.handler = async (event) => {
    console.log("DynamoDB Stream Event: ", JSON.stringify(event, null, 2));

    try {
        // Loop through all records in the Stream
        for (const record of event.Records) {
            // Only handle INSERT or MODIFY events
            if (record.eventName === "INSERT" || record.eventName === "MODIFY") {
                const newImage = record.dynamodb.NewImage;
                const oldImage = record.dynamodb.OldImage;

                // Extract the 'id' field (primary key) from the record
                const recordId = newImage.id.S;

                // Generate a unique timestamp for the audit entry
                const timestamp = new Date().toISOString();

                // Determine the action type and details
                let action = "";
                let details = "";
                if (record.eventName === "INSERT") {
                    action = "INSERT";
                    details = `New item created: ${JSON.stringify(newImage)}`;
                } else if (record.eventName === "MODIFY") {
                    action = "MODIFY";
                    details = `Item modified from: ${JSON.stringify(oldImage || {})} to: ${JSON.stringify(newImage || {})}`;
                }

                // Construct the audit entry
                const auditEntry = {
                    id: `${timestamp}_${recordId}`, // Unique ID for each audit entry
                    recordId: recordId,
                    action: action,
                    details: details,
                    timestamp: timestamp,
                };

                // Write the audit entry to the Audit table
                await dynamodb.put({
                    TableName: AUDIT_TABLE_NAME,
                    Item: auditEntry,
                }).promise();

                console.log("Audit entry created: ", JSON.stringify(auditEntry));
            }
        }

        return {
            statusCode: 200,
            body: JSON.stringify("Stream processed successfully"),
        };
    } catch (error) {
        console.error("Error processing DynamoDB Stream: ", error);

        return {
            statusCode: 500,
            body: JSON.stringify("Error processing stream"),
        };
    }
};