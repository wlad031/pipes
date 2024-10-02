package dev.vgerasimov.pipes.steps;

import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import dev.vgerasimov.pipes.BaseStep;
import dev.vgerasimov.pipes.Step;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
public class InfluxSinkStep extends BaseStep implements Step {

  private static final WriteOptions DEFAULT_WRITE_OPTIONS;

  static {
    var defaultWriteBatchSize = 1000;
    var defaultWriteBufferLimit = 1000000;
    DEFAULT_WRITE_OPTIONS =
        WriteOptions.builder()
            .batchSize(defaultWriteBatchSize)
            .bufferLimit(defaultWriteBufferLimit)
            .build();
  }

  private final InfluxDBClient influxDBClient;
  private final WriteApi writeApi;

  private final ObjectMapper objectMapper = new ObjectMapper();

  protected InfluxSinkStep(
      String name, InfluxDBClient influxDBClient, @Nullable WriteOptions writeOptions) {
    super(name);

    this.influxDBClient = influxDBClient;
    this.writeApi =
        influxDBClient.makeWriteApi(writeOptions != null ? writeOptions : DEFAULT_WRITE_OPTIONS);

    boolean pong = influxDBClient.ping();
    if (!pong) {
      throw new IllegalStateException("InfluxDB ping returned false");
    }

    log.info("InfluxDB initialized");
  }

  @SneakyThrows
  @Override
  public Flux<Map<String, Object>> apply(Map<String, Object> payload) {
    if (!influxDBClient.ping()) {
      return Flux.error(new IllegalStateException("InfluxDB ping returned false"));
    }

    if (payload == null) {
      log.error("Payload is null");
      return Flux.empty();
    }
    if (payload.isEmpty()) {
      log.warn("Payload is empty");
      return Flux.just(payload);
    }

    var measurements = objectMapper.convertValue(payload, Measurements.class).measurements();
    log.info("Got {} measurements", measurements.size());

    ArrayList<Point> points = new ArrayList<>();

    for (var measurement : measurements) {
      var point =
          Point.measurement(measurement.name).time(measurement.timestamp(), WritePrecision.S);

      for (var entry : measurement.fields()) {
        point.addField(entry.name(), entry.value());
      }

      point.addTags(
          measurement.tags().entrySet().stream()
              .filter(e -> e.getKey() != null && e.getValue() != null)
              .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));

      points.add(point);
    }

    writeApi.writePoints(points);
    log.info("Wrote {} points", points.size());

    writeApi.flush();
    log.info("Flushed");

    return Flux.just(Map.of("points", points.size()));
  }

  record Measurements(List<Measurement> measurements) {}

  record Measurement(String name, long timestamp, List<Field> fields, Map<String, String> tags) {
    record Field(String name, BigDecimal value) {}
  }
}
