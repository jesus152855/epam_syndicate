package com.task09;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.*;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
        lambdaName = "processor",
        roleName = "api_handler-role",
        layers = {"open-meteo-api-layer"},
        runtime = DeploymentRuntime.JAVA17,
        architecture = Architecture.ARM64,
        tracingMode = TracingMode.Active,
        logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaLayer(
        layerName = "open-meteo-api-layer",
        libraries = {"lib/httpclient-4.5.13.jar", "lib/httpcore-4.4.13.jar"},
        runtime = DeploymentRuntime.JAVA17,
        architectures = {Architecture.ARM64},
        artifactExtension = ArtifactExtension.ZIP
)
@LambdaUrlConfig(
        authType = AuthType.NONE,
        invokeMode = InvokeMode.BUFFERED
)
@EnvironmentVariables(value = {
        @EnvironmentVariable(key = "region", value = "${region}"),
        @EnvironmentVariable(key = "table", value = "${target_table}")})
public class WeatherHandler implements RequestHandler<Void, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";

    private final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(System.getenv("region")).build();

    @Override
    public String handleRequest(Void event, Context context) {
       var response = HttpClient.executeGet(URL);
        try {
            var weatherResponse = objectMapper.readValue(response, WeatherResponse.class);
            persistData(weatherResponse);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return response;
    }

    private void persistData(WeatherResponse weatherResponse) {
        var attributesMap = new HashMap<String, AttributeValue>();
        attributesMap.put("id", new AttributeValue(String.valueOf(UUID.randomUUID().toString())));
        attributesMap.put("forecast", new AttributeValue().withM(buildBodyValue(weatherResponse)));
        amazonDynamoDB.putItem(System.getenv("table"), attributesMap);
    }

    private Map<String, AttributeValue> buildBodyValue(WeatherResponse weatherResponse) {
        var attributesMapForecast = new HashMap<String, AttributeValue>();
        attributesMapForecast.put("elevation", new AttributeValue().withN(String.valueOf(weatherResponse.elevation())));
        attributesMapForecast.put("generationtime_ms", new AttributeValue().withN(String.valueOf(weatherResponse.generationtime_ms())));

        var attributesMapHourly = new HashMap<String, AttributeValue>();
        attributesMapHourly.put("temperature_2m", new AttributeValue().withL(
                weatherResponse.hourly().temperature_2m().stream().map(value -> new AttributeValue().withN(String.valueOf(value))).toList())
        );
        attributesMapHourly.put("time", new AttributeValue().withL(
                weatherResponse.hourly().time().stream().map(AttributeValue::new).toList())
        );
        attributesMapForecast.put("hourly", new AttributeValue().withM(attributesMapHourly));
        var attributesMapHourlyUnits = new HashMap<String, AttributeValue>();
        attributesMapHourlyUnits.put("temperature_2m", new AttributeValue(weatherResponse.hourly_units().temperature_2m()));
        attributesMapHourlyUnits.put("time", new AttributeValue(weatherResponse.hourly_units().time()));
        attributesMapHourlyUnits.put("hourly_units", new AttributeValue().withM(attributesMapHourlyUnits));

        attributesMapForecast.put("latitude", new AttributeValue().withN(String.valueOf(weatherResponse.latitude())));
        attributesMapForecast.put("longitude", new AttributeValue().withN(String.valueOf(weatherResponse.longitude())));
        attributesMapForecast.put("timezone", new AttributeValue(String.valueOf(weatherResponse.timezone())));
        attributesMapForecast.put("timezone_abbreviation",new AttributeValue(String.valueOf(weatherResponse.timezone_abbreviation())));
        attributesMapForecast.put("utc_offset_seconds",new AttributeValue().withN(String.valueOf(weatherResponse.utc_offset_seconds())));
        return attributesMapForecast;
    }

    public record Hourly(List<Number> temperature_2m, List<String> time, List<Number> relative_humidity_2m, List<Number> wind_speed_10m) {

    }

    public record HourlyUnits(String temperature_2m, String time, String relative_humidity_2m, String wind_speed_10m) {

    }

    public record CurrentUnits(Number temperature, String time, String interval, String temperature_2m, String wind_speed_10m) {

    }

    public record Current(Number temperature, String time, String interval, String temperature_2m, String wind_speed_10m) {

    }

    public record WeatherResponse(Number latitude, Number longitude, Number utc_offset_seconds, Number elevation, Number generationtime_ms,
                                  CurrentUnits current_units, Current current,
                                  Hourly hourly, HourlyUnits hourly_units,
                                  String timezone, String timezone_abbreviation) {
    }
}
