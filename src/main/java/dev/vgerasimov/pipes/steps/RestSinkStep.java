package dev.vgerasimov.pipes.steps;

import dev.vgerasimov.pipes.BaseStep;
import dev.vgerasimov.pipes.Step;
import java.util.Map;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

public class RestSinkStep extends BaseStep implements Step {

  private final WebClient restClient;

  public RestSinkStep(String name, WebClient restClient) {
    super(name);
    this.restClient = restClient;
  }

  @Override
  public Flux<Map<String, Object>> apply(Map<String, Object> payload) {
    if (payload == null) {
      log.error("Payload is null");
      return Flux.empty();
    }
    if (payload.isEmpty()) {
      log.warn("Payload is empty");
      return Flux.just(payload);
    }
    log.debug("Sending payload: {}", payload);
    return restClient
        .post()
        .bodyValue(payload)
        .retrieve()
        .bodyToFlux(Map.class)
        .map(map -> (Map<String, Object>) map)
        .doOnNext(s -> log.debug("Response: {}", s))
        .doOnError(t -> log.error("Got error", t))
        .onErrorResume(t -> Flux.empty());
  }
}
