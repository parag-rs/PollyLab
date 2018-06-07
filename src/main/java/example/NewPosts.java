package example;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;

import java.util.UUID;

/**
 * @author Parag Sagdeo
 */
//Lambda Function Basic settings: Memory 256MB, Timeout 25 Seconds
public class NewPosts implements RequestHandler<RequestDataNewPosts, String> {

    public String handleRequest(RequestDataNewPosts requestData, Context context) {
        LambdaLogger logger = context.getLogger();
        String recordId = UUID.randomUUID().toString();
        logger.log("Generating new DynamoDB record, with ID: " + recordId);
        logger.log("Input Text: " + requestData.getText());
        logger.log("Selected Voice: " + requestData.getVoice());
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(System.getenv("DB_TABLE_NAME"));
        Item item = new Item()
                .withPrimaryKey("id", recordId)
                .withString("text", requestData.getText())
                .withString("voice", requestData.getVoice())
                .withString("status", "PROCESSING");
        table.putItem(item);
        AmazonSNS snsClient = AmazonSNSClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .build();
        PublishRequest publishRequest = new PublishRequest(System.getenv("SNS_TOPIC"), recordId);
        snsClient.publish(publishRequest);
        return recordId;
    }
}