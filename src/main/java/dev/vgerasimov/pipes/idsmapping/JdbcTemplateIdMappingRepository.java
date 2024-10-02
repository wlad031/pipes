package dev.vgerasimov.pipes.idsmapping;

import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@RequiredArgsConstructor
public class JdbcTemplateIdMappingRepository implements IdMappingRepository {
  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Override
  public Map<String, String> findMappings(String type, String id) {
    return jdbcTemplate.query(
        """
        SELECT service_type AS type, service_id AS id, global_id AS global_id
        FROM id_mapping
        WHERE global_id IN (
          SELECT DISTINCT global_id FROM id_mapping WHERE service_type = :t AND service_id = :i
        )
        """,
        new MapSqlParameterSource().addValue("i", id).addValue("t", type),
        rs -> {
          var map = new HashMap<String, String>();
          String globalId = null;
          while (rs.next()) {
            String rowGlobalId = rs.getString("global_id");
            if (globalId == null) {
              globalId = rowGlobalId;
            } else if (!globalId.equals(rowGlobalId)) {
              throw new IllegalStateException(
                  String.format(
                      "Found multiple global ids for service type %s and id %s: %s, %s",
                      type, id, globalId, rowGlobalId));
            }
            map.put(rs.getString("type"), rs.getString("id"));
          }
          map.put("global", globalId);
          return map;
        });
  }

  @Override
  public void saveMapping(String globalId, String type, String id) {
    jdbcTemplate.update(
        """
        INSERT INTO id_mapping (global_id, service_type, service_id)
        VALUES (:globalId, :type, :id)
        ON CONFLICT (service_type, service_id) DO NOTHING
        """,
        new MapSqlParameterSource()
            .addValue("globalId", globalId)
            .addValue("type", type)
            .addValue("id", id));
  }
}
