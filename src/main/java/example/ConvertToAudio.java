package example;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.polly.AmazonPolly;
import com.amazonaws.services.polly.AmazonPollyClientBuilder;
import com.amazonaws.services.polly.model.OutputFormat;
import com.amazonaws.services.polly.model.SynthesizeSpeechRequest;
import com.amazonaws.services.polly.model.SynthesizeSpeechResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Parag Sagdeo
 */
//Lambda Function Basic settings: Memory 256MB, Timeout 25 Seconds
// Grant  "dynamodb:GetItem" to Lambda Role
public class ConvertToAudio implements RequestHandler<SNSEvent, String> {

    public String handleRequest(SNSEvent event, Context context) {
        String postId = event.getRecords().get(0).getSNS().getMessage();
        LambdaLogger logger = context.getLogger();
        logger.log("Text to Speech function. Post ID in DynamoDB: " + postId);
        //Retrieving information about the post from DynamoDB table
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .build();
        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
        Table table = dynamoDB.getTable(System.getenv("DB_TABLE_NAME"));
        Item item = table.getItem("id", postId);
        String text = item.getString("text");
        String voice = item.getString("voice");
        //Because single invocation of the polly synthesize_speech api can
        //transform text with about 1,500 characters, we are dividing the
        //post into blocks of approximately 1,000 characters.
        String rest = text;
        List<String> textBlocks = new ArrayList<String>();

        while (rest.length() > 1100) {
            int begin = 0;
            int end = rest.indexOf('.');
            if (end == -1) {
                end = rest.indexOf(' ', 1000);
            }
            String textBlock = rest.substring(begin, end);
            rest = rest.substring(end);
            textBlocks.add(textBlock);
        }
        textBlocks.add(rest);
        //For each block, invoke Polly API, which will transform text into audio
        AmazonPolly polly = AmazonPollyClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .build();

        textBlocks.forEach(textBlock -> {
            SynthesizeSpeechRequest synthesizeSpeechRequest = new SynthesizeSpeechRequest()
                    .withOutputFormat(OutputFormat.Mp3)
                    .withVoiceId(voice)
                    .withText(textBlock);
            //Save the audio stream returned by Amazon Polly on Lambda's temp
            //directory. If there are multiple text blocks, the audio stream
            //will be combined into a single file.
            String outputFileName = "/tmp/" + postId;
            try (FileOutputStream outputStream = new FileOutputStream(new File(outputFileName), true)) {
                SynthesizeSpeechResult synthesizeSpeechResult = polly.synthesizeSpeech(synthesizeSpeechRequest);
                byte[] buffer = new byte[2 * 1024];
                int readBytes;
                try (InputStream in = synthesizeSpeechResult.getAudioStream()) {
                    while ((readBytes = in.read(buffer)) > 0) {
                        outputStream.write(buffer, 0, readBytes);
                    }
                }
            } catch (Exception e) {
                logger.log("Exception caught: " + e);
            }
        });

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .build();

        s3Client.putObject(new PutObjectRequest(System.getenv("BUCKET_NAME"), postId + ".mp3", new File("/tmp/" + postId))
                .withCannedAcl(CannedAccessControlList.PublicRead));

        String bucketLocation = s3Client.getBucketLocation(new GetBucketLocationRequest(System.getenv("BUCKET_NAME")));
        String url_begining;
        if (bucketLocation != null && bucketLocation.trim().length() > 0) {
            url_begining = "https://s3-" + bucketLocation + ".amazonaws.com/";
        } else {
            url_begining = "https://s3.amazonaws.com/";
        }
        String url = url_begining
                + System.getenv("BUCKET_NAME")
                + "/"
                + postId
                + ".mp3";
        //Updating the item in DynamoDB
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("id", postId)
                .withUpdateExpression("set #statusAtt = :statusValue, #urlAtt = :urlValue")
                .withNameMap(new NameMap().with("#statusAtt", "status").with("#urlAtt", "url"))
                .withValueMap(new ValueMap().withString(":statusValue", "UPDATED").withString(":urlValue", url))
                .withReturnValues(ReturnValue.ALL_NEW);

        table.updateItem(updateItemSpec);

        return null;
    }
}