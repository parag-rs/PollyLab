package example;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.List;

/**
 * @author Parag Sagdeo
 */
//Lambda Function Basic settings: Memory 256MB, Timeout 25 Seconds
public class GetPosts implements RequestHandler<RequestDataGetPosts, List<ResponseDataGetPosts>> {

    public List<ResponseDataGetPosts> handleRequest(RequestDataGetPosts requestData, Context context) {
        LambdaLogger logger = context.getLogger();
        String postId = requestData.getPostId();
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .build();

        DynamoDBMapper mapper = new DynamoDBMapper(client);
        List<ResponseDataGetPosts> itemList;
        if (postId.equals("*")) {
            itemList = mapper.scan(ResponseDataGetPosts.class, new DynamoDBScanExpression());
        } else {
            ResponseDataGetPosts responseDataGetPosts = new ResponseDataGetPosts();
            responseDataGetPosts.setId(postId);
            DynamoDBQueryExpression<ResponseDataGetPosts> queryExpression = new DynamoDBQueryExpression<ResponseDataGetPosts>()
                    .withHashKeyValues(responseDataGetPosts);

            itemList = mapper.query(ResponseDataGetPosts.class, queryExpression);
        }
        return itemList;
    }
}