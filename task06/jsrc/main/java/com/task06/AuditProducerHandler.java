package com.task06;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

@LambdaHandler(lambdaName = "audit_producer",
	roleName = "dynamodb_handler-role",
    runtime = DeploymentRuntime.JAVA17,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariables(value = {
        @EnvironmentVariable(key = "region", value = "${region}"),
        @EnvironmentVariable(key = "targetTable", value = "${target_table}")})
@DynamoDbTriggerEventSource(targetTable = "Configuration", batchSize = 10)
public class AuditProducerHandler implements RequestHandler<DynamodbEvent, Void> {

    private final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(System.getenv("region")).build();

    @Override
    public Void handleRequest(DynamodbEvent dynamodbEvent, Context context) {
        for (DynamodbEvent.DynamodbStreamRecord record : dynamodbEvent.getRecords()) {
            try {
                var auditEvent = buildAuditEvent(record);
                persistData(auditEvent);
            } catch (Exception e) {
                System.err.format("Error saving the item to DynamoDB because %s", e.getMessage());
            }
        }
        return null;
    }

    private AuditEvent buildAuditEvent(DynamodbEvent.DynamodbStreamRecord record) {
        var uuid = UUID.randomUUID();
        var uuidAsString = uuid.toString();
        var modificationTime = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();
        var key = record.getDynamodb().getKeys().get("key").getS();
        var attributeValueNew = record.getDynamodb().getNewImage().get("value").getN();
        var configuration = new Configuration(key, Integer.parseInt(attributeValueNew));
        if(record.getEventName().equals("INSERT")) {
            return new AuditEvent(uuidAsString, key, modificationTime, null, configuration, null);
        }
        var attributeValueOld = record.getDynamodb().getOldImage().get("value").getN();
        return new AuditEvent(uuidAsString, key, modificationTime, "value", configuration,
                Integer.parseInt(attributeValueOld));
    }

    private void persistData(AuditEvent auditEvent) {
        var attributesMap = new HashMap<String, AttributeValue>();
        attributesMap.put("id", new AttributeValue(String.valueOf(auditEvent.id())));
        attributesMap.put("itemKey", new AttributeValue(String.valueOf(auditEvent.itemKey())));
        attributesMap.put("modificationTime", new AttributeValue(String.valueOf(auditEvent.modificationTime())));
        attributesMap.put("newValue", new AttributeValue().withM(buildBodyValue(auditEvent)));
        if(Objects.nonNull(auditEvent.oldValue())) {
            attributesMap.put("updatedAttribute", new AttributeValue(String.valueOf(auditEvent.updatedAttribute())));
            attributesMap.put("oldValue", new AttributeValue().withN(String.valueOf(auditEvent.oldValue())));
            attributesMap.put("newValue", new AttributeValue().withN(String.valueOf(auditEvent.configuration().value())));
        }
        System.out.println("Event to persist: " + attributesMap);
        amazonDynamoDB.putItem(System.getenv("targetTable"), attributesMap);
    }

    private Map<String, AttributeValue> buildBodyValue(AuditEvent auditEvent) {
        var attributesMap = new HashMap<String, AttributeValue>();
        attributesMap.put("key", new AttributeValue(auditEvent.configuration().key()));
        attributesMap.put("value", new AttributeValue().withN(String.valueOf(auditEvent.configuration().value())));
        return attributesMap;
    }

    private record AuditEvent(String id, String itemKey, String modificationTime, String updatedAttribute, Configuration configuration, Integer oldValue) {

    }

    public record Configuration(String key, Integer value) {

    }

}
