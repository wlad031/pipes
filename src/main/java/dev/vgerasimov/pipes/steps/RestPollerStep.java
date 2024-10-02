package dev.vgerasimov.pipes.steps;

import dev.vgerasimov.common.time.DateTimesService;
import dev.vgerasimov.pipes.BaseStep;
import dev.vgerasimov.pipes.SourceStep;
import dev.vgerasimov.pipes.registrar.properties.Schedule;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Builder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RestPollerStep extends BaseStep implements SourceStep {

  private final WebClient restClient;
  private final Schedule schedule;
  private final Clock clock;
  private final DateTimesService dateTimesService;

  public RestPollerStep(
      String name,
      WebClient restClient,
      Schedule schedule,
      Clock clock,
      DateTimesService dateTimesService) {
    super(name);
    this.restClient = restClient;
    this.schedule = schedule;
    this.clock = clock;
    this.dateTimesService = dateTimesService;
  }

  @Override
  public Flux<Map<String, Object>> apply() {
    return Flux.interval(((Schedule.Interval) schedule).interval())
        .flatMap(
            __ -> {
              OffsetDateTime now = OffsetDateTime.now(clock);
              OffsetDateTime to = now.truncatedTo(ChronoUnit.SECONDS);
              OffsetDateTime from = to.minus(schedule.window());
              return apply(Map.of("from", from, "to", to));
            })
        .doOnError(t -> log.error("Got error", t))
        .onErrorResume(t -> Mono.empty());
  }

  @Override
  public Flux<Map<String, Object>> apply(Map<String, Object> payload) {
    var from =
        Optional.ofNullable(payload.get("from"))
            .filter(x -> x instanceof String)
            .map(String.class::cast)
            .map(this::parseAsOffsetDateTime)
            .orElse(OffsetDateTime.now().minusMonths(100));
    var to =
        Optional.ofNullable(payload.get("to"))
            .filter(x -> x instanceof String)
            .map(String.class::cast)
            .map(this::parseAsOffsetDateTime)
            .orElse(OffsetDateTime.now().plusMonths(100));
    return restClient
        .get()
        .uri(
            builder ->
                builder
                    .queryParam("afterInclusive", from.toEpochSecond())
                    .queryParam("beforeExclusive", to.toEpochSecond())
                    .build())
        .retrieve()
        .bodyToFlux(new ParameterizedTypeReference<ListDto<Map<String, Object>>>() {})
        .flatMap(
            listDto -> Flux.fromIterable(Optional.ofNullable(listDto.data()).orElseGet(List::of)));
  }

  @Nullable
  private OffsetDateTime parseAsOffsetDateTime(@Nullable String value) {
    if (value == null) return null;
    return dateTimesService
        .safeParseAsOffsetDateTime(value)
        .orElseThrow(() -> new RuntimeException("This should be a bad request instead"));
  }

  @Builder
  record ListDto<T>(List<T> data) {}
}
