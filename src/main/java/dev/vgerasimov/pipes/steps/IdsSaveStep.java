package dev.vgerasimov.pipes.steps;

import dev.vgerasimov.pipes.BaseStep;
import dev.vgerasimov.pipes.idsmapping.IdMappingRepository;
import dev.vgerasimov.pipes.Step;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import reactor.core.publisher.Flux;

public class IdsSaveStep extends BaseStep implements Step {

  private final IdMappingRepository idMappingRepository;

  public IdsSaveStep(String name, IdMappingRepository idMappingRepository) {
    super(name);
    this.idMappingRepository = idMappingRepository;
  }

  @Override
  public Flux<Map<String, Object>> apply(@Nullable Map<String, Object> payload) {
    if (payload == null) {
      log.error("Payload is null");
      return Flux.empty();
    }
    if (payload.isEmpty()) {
      log.warn("Payload is empty");
      return Flux.just(payload);
    }

    var ids = payload.get("ids");
    if (ids == null) {
      log.warn("Payload doesn't contain ids");
      return Flux.just(payload);
    }
    Map<String, String> mapIds;
    try {
      mapIds = (Map<String, String>) ids;
    } catch (ClassCastException e) {
      log.error("Payload ids is not a map");
      return Flux.just(payload);
    }

    if (mapIds.isEmpty()) {
      log.warn("Payload doesn't contain any ids");
      return Flux.just(payload);
    }

    String globalId = mapIds.get("global");
    if (globalId == null) {
      globalId = UUID.randomUUID().toString();
    }
    String finalGlobalId = globalId;
    mapIds.forEach(
        (k, v) -> {
          if (!k.equals("global")) {
            log.info("Saving mapping id {} of type {} for global id {}", v, k, finalGlobalId);
            idMappingRepository.saveMapping(finalGlobalId, k, v);
          }
        });

    payload.put("ids", mapIds);

    return Flux.just(payload);
  }
}
