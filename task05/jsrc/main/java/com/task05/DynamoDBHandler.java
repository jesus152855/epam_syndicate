package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
            var createdAt = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();
            var eventRequest = objectMapper.readValue(requestEvent.getBody(), EventRequest.class);
            var event = new Event(uuidAsString, eventRequest.principalId(), createdAt, eventRequest.content());
            persistData(event);
            return buildResponse(201, event);
        } catch (Exception e) {
            System.err.format("Error saving the item to DynamoDB because %s", e.getMessage());
        }
        return buildResponse(500, null);
    }

    private void persistData(Event event) {
        var attributesMap = new HashMap<String, AttributeValue>();
        attributesMap.put("id", new AttributeValue(String.valueOf(event.id())));
        attributesMap.put("principalId", new AttributeValue().withN(String.valueOf(event.principalId())));
        attributesMap.put("body", new AttributeValue().withM(buildBodyValue(event)));
        attributesMap.put("createdAt", new AttributeValue(String.valueOf(event.createdAt())));
        amazonDynamoDB.putItem(System.getenv("table"), attributesMap);
    }

    private Map<String, AttributeValue> buildBodyValue(Event event) {
        var attributesMap = new HashMap<String, AttributeValue>();
        for(Map.Entry<String, String> entry: event.body().entrySet()) {
           attributesMap.put(entry.getKey(), new AttributeValue(entry.getValue()));
        }
        return attributesMap;
    }

    private APIGatewayV2HTTPResponse buildResponse(Integer statusCode, Event event) {
        try {
            var eventResponse = new EventResponse(statusCode, event);
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(statusCode)
                    .withBody(Objects.isNull(event) ? "Error processing the request" : objectMapper.writeValueAsString(eventResponse))
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

    public record EventResponse(Integer statusCode, Event event) {

    }
}
