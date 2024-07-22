package com.task07;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.RuleEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Stream;

@LambdaHandler(lambdaName = "uuid_generator",
	roleName = "cloudwatch_handler-role",
    runtime = DeploymentRuntime.JAVA17,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariables(value = {
        @EnvironmentVariable(key = "region", value = "${region}"),
        @EnvironmentVariable(key = "targetBucket", value = "${target_bucket}")})
@RuleEventSource(targetRule = "${source_rule}")
public class CloudWatchRuleHandler implements RequestHandler<ScheduledEvent, List<String>> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final  AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(System.getenv("region")).build();

    @Override
    public List<String> handleRequest(ScheduledEvent cloudWatchLogsEvent, Context context) {
        var modificationTime = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();
        var randomIds = Stream.generate(() -> UUID.randomUUID().toString()).limit(10).toList();
        var contentIds = new FileContent(randomIds);
        String fileContent;
        try {
            fileContent = objectMapper.writeValueAsString(contentIds);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        s3Client.putObject(System.getenv("targetBucket"), modificationTime, fileContent);
        return List.of();
    }

    public record FileContent(List<String> ids) {
    }
}
