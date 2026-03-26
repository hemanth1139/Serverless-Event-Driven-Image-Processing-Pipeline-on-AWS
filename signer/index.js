const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const { randomUUID } = require("crypto");

const s3Client = new S3Client({ region: process.env.AWS_REGION || "ap-south-1" });

exports.handler = async (event) => {
  const corsHeaders = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "OPTIONS,POST",
  };

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: corsHeaders, body: "" };
  }

  try {
    const body = JSON.parse(event.body);
    const { fileName, fileType } = body;

    const allowedTypes = ["image/jpeg", "image/png"];
    if (!allowedTypes.includes(fileType)) {
      return {
        statusCode: 400,
        headers: corsHeaders,
        body: JSON.stringify({ error: "Only JPG and PNG files are allowed." }),
      };
    }

    const fileExtension = fileType === "image/jpeg" ? "jpg" : "png";
    const uniqueKey = `uploads/${randomUUID()}.${fileExtension}`;

    const command = new PutObjectCommand({
      Bucket: process.env.SOURCE_BUCKET,
      Key: uniqueKey,
      ContentType: fileType,
    });

    // Pre-signed URL expires in 5 minutes
    const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 300 });

    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({ uploadUrl, key: uniqueKey }),
    };
  } catch (err) {
    console.error("Error generating pre-signed URL:", err);
    return {
      statusCode: 500,
      headers: corsHeaders,
      body: JSON.stringify({ error: "Failed to generate upload URL." }),
    };
  }
};
