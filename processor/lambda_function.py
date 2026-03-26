import boto3
import os
import uuid
from datetime import datetime, timezone
from io import BytesIO
from PIL import Image

s3        = boto3.client("s3")
dynamodb  = boto3.resource("dynamodb")
sns       = boto3.client("sns")

SOURCE_BUCKET = os.environ["SOURCE_BUCKET"]
DEST_BUCKET   = os.environ["DEST_BUCKET"]
DYNAMO_TABLE  = os.environ["DYNAMO_TABLE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

MAX_SIZE = (800, 800)   # resized image fits within this box, aspect ratio preserved


def lambda_handler(event, context):
    for record in event["Records"]:
        source_key        = record["s3"]["object"]["key"]
        original_filename = source_key.split("/")[-1]

        print(f"Processing: {source_key}")

        # 1. Download original from source bucket
        response     = s3.get_object(Bucket=SOURCE_BUCKET, Key=source_key)
        image_data   = response["Body"].read()
        content_type = response["ContentType"]

        # 2. Validate type
        if content_type not in ["image/jpeg", "image/png"]:
            print(f"Unsupported type: {content_type} — skipping.")
            continue

        # 3. Open with Pillow
        image = Image.open(BytesIO(image_data))

        # 4. Handle PNG transparency
        if image.mode == "RGBA":
            background = Image.new("RGB", image.size, (255, 255, 255))
            background.paste(image, mask=image.split()[3])
            image = background
        elif image.mode != "RGB":
            image = image.convert("RGB")

        # 5. Resize — maintains aspect ratio
        image.thumbnail(MAX_SIZE, Image.LANCZOS)

        # 6. Save to buffer
        buffer      = BytesIO()
        save_format = "PNG" if content_type == "image/png" else "JPEG"
        image.save(buffer, format=save_format, quality=85, optimize=True)
        buffer.seek(0)

        # 7. Upload resized image to destination bucket
        resized_key = f"resized/{original_filename}"
        s3.put_object(
            Bucket=DEST_BUCKET,
            Key=resized_key,
            Body=buffer,
            ContentType=content_type,
        )
        print(f"Resized image saved → s3://{DEST_BUCKET}/{resized_key}")

        # 8. Store metadata in DynamoDB
        image_id  = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()

        table = dynamodb.Table(DYNAMO_TABLE)
        table.put_item(
            Item={
                "imageId":          image_id,
                "originalFilename": original_filename,
                "sourceKey":        source_key,
                "resizedKey":       resized_key,
                "status":           "PROCESSED",
                "processedAt":      timestamp,
            }
        )
        print(f"Metadata saved — imageId: {image_id}")

        # 9. Send SNS email
        message = (
            f"Image Processing Complete\n"
            f"{'─' * 35}\n"
            f"Filename   : {original_filename}\n"
            f"Status     : PROCESSED\n"
            f"Timestamp  : {timestamp}\n"
            f"Output     : s3://{DEST_BUCKET}/{resized_key}\n"
        )
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Image Processed Successfully",
            Message=message,
        )
        print("SNS notification sent.")

    return {"statusCode": 200, "body": "Processing complete."}
