package dev.vgerasimov.pipes.steps;

import dev.vgerasimov.pipes.BaseStep;
import dev.vgerasimov.pipes.Step;
import java.util.Map;
import reactor.core.publisher.Flux;

public class EchoStep extends BaseStep implements Step {
  
  public EchoStep(String name) {
    super(name);
  }

  @Override
  public Flux<Map<String, Object>> apply(Map<String, Object> payload) {
    log.info("Payload: {}", payload);
    return Flux.just(payload);
  }
}
