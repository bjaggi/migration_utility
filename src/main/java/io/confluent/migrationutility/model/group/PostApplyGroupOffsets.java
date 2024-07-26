package io.confluent.migrationutility.model.group;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PostApplyGroupOffsets {
  private String consumerGroupName;
  private Map<String, Long> tpOffsets;

}
