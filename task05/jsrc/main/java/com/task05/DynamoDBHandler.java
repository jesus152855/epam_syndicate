package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@LambdaHandler(lambdaName = "api_handler",
	roleName = "dynamodb_handler-role",
    runtime = DeploymentRuntime.JAVA17,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariables(value = {
        @EnvironmentVariable(key = "region", value = "${region}"),
        @EnvironmentVariable(key = "table", value = "${target_table}")})
public class DynamoDBHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

	private final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(System.getenv("region")).build();
    private final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent requestEvent, Context context) {
        try {
            var uuid = UUID.randomUUID();
            var uuidAsString = uuid.toString();
            var createdAt = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
            var eventRequest = objectMapper.readValue(requestEvent.getBody(), EventRequest.class);
            var event = new Event(uuidAsString, eventRequest.principalId(), createdAt, eventRequest.content());
            persistData(event);
            return buildResponse(201, event);
        } catch (Exception e) {
            System.err.format("Error saving the item to DynamoDB because %s", e.getMessage());
        }
        return buildResponse(500, null);
    }

    private void persistData(Event event) throws JsonProcessingException {
        var attributesMap = new HashMap<String, AttributeValue>();
        attributesMap.put("id", new AttributeValue(String.valueOf(event.id())));
        attributesMap.put("principalId", new AttributeValue(String.valueOf(event.principalId())));
        attributesMap.put("body", new AttributeValue(String.valueOf(objectMapper.writeValueAsString(event.body()))));
        attributesMap.put("createdAt", new AttributeValue(String.valueOf(event.createdAt())));
        amazonDynamoDB.putItem(System.getenv("table"), attributesMap);
    }

    private APIGatewayV2HTTPResponse buildResponse(int statusCode, Event event) {
        try {
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(statusCode)
                    .withBody(Objects.isNull(event) ? "Error processing the request" : objectMapper.writeValueAsString(event))
                    .build();
        } catch (Exception e) {
            System.err.format("Error mapping the response because %s", e.getMessage());
        }
        return APIGatewayV2HTTPResponse.builder()
                .withStatusCode(500)
                .build();
    }


    private record Event(String id, Integer principalId, String createdAt, Map<String, String>  body) {

    }

    public record EventRequest(Integer principalId, Map<String, String> content) {

    }


}
