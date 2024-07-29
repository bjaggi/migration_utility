package io.confluent.migrationutility.model.quota;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class QuotaMetadataExtendedRequest {
    private String clusterId;
    private List<QuotaEntry> quotaEntries;
}
