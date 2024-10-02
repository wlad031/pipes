package dev.vgerasimov.pipes.idsmapping;

import java.util.Map;

public interface IdMappingRepository {
  
  Map<String, String> findMappings(String type, String id);

  void saveMapping(String globalId, String type, String id);
}
