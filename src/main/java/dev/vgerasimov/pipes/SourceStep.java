package dev.vgerasimov.pipes;

import java.util.Map;
import reactor.core.publisher.Flux;

public interface SourceStep extends Step {

  @Override
  default Flux<Map<String, Object>> apply(Map<String, Object> ignored) {
    return apply();
  }

  Flux<Map<String, Object>> apply();
}
