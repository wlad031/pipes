package dev.vgerasimov.pipes.registrar.properties;

import java.time.Duration;
import javax.annotation.Nullable;

public record StepProperties(
    String type,
    String name,
    @Nullable String restUrl,
    @Nullable Schedule restSchedule,
    @Nullable String kafkaBootstrapServers,
    @Nullable String kafkaTopic,
    @Nullable String sourceIdType,
    @Nullable String className,
    @Nullable String influxUrl,
    @Nullable String influxToken,
    @Nullable String influxOrg,
    @Nullable String influxBucket,
    @Nullable Integer influxBatchSize,
    @Nullable Integer influxBufferLimit,
    @Nullable String custom
) {

  public record Schedule(
      String type,
      Duration window,
      @Nullable Duration interval
  ) {}
}
