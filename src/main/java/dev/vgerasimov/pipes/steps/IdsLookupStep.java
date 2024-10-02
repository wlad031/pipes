package dev.vgerasimov.pipes.steps;

import dev.vgerasimov.pipes.BaseStep;
import dev.vgerasimov.pipes.idsmapping.IdMappingRepository;
import dev.vgerasimov.pipes.Step;
import java.util.Map;
import reactor.core.publisher.Flux;

public class IdsLookupStep extends BaseStep implements Step {

  private final IdMappingRepository idMappingRepository;
  private final String sourceIdType;

  public IdsLookupStep(String name, IdMappingRepository idMappingRepository, String sourceIdType) {
    super(name);
    this.idMappingRepository = idMappingRepository;
    this.sourceIdType = sourceIdType;
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

    var sourceId = mapIds.get(sourceIdType);
    if (sourceId == null) {
      log.warn("Payload doesn't contain an id of type {}", sourceIdType);
      return Flux.just(payload);
    }

    var mappings = idMappingRepository.findMappings(sourceIdType, sourceId);
    log.info(
        "Found {} mappings for id {} of type {}: {}",
        mappings.size(),
        sourceId,
        sourceIdType,
        mappings);

    mapIds.putAll(mappings);
    payload.put("ids", mapIds);

    return Flux.just(payload);
  }
}
