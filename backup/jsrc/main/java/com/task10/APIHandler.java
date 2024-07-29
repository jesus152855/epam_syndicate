package com.task10;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;

@LambdaHandler(lambdaName = "api_handler",
	roleName = "api_handler-role",
    runtime = DeploymentRuntime.JAVA17,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariables(value = {
        @EnvironmentVariable(key = "region", value = "${region}"),
        @EnvironmentVariable(key = "table", value = "${target_table}")})
public class APIHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(System.getenv("region")).build();

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent requestEvent, Context context) {
        System.out.println("API request:" + requestEvent);
        var httpMethod = requestEvent.getRequestContext().getHttp().getMethod();
        var httpPath = requestEvent.getRequestContext().getHttp().getPath();
        System.out.println("Values: " + httpPath + " - " + httpMethod);
        try {
            var userRequest = objectMapper.readValue(requestEvent.getBody(), UserRequest.class);
            System.out.println("User request : " + userRequest);
        } catch (JsonProcessingException e) {
            return APIGatewayV2HTTPResponse.builder().withBody("ERROR").build();
        }
        return APIGatewayV2HTTPResponse.builder().withBody("OK").build();
    }

    public record UserRequest(String firstName, String lastName, String email, String password) {

    }
}
