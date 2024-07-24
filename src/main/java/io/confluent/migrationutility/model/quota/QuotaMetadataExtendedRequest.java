package io.confluent.migrationutility.model.quota;

import lombok.Data;

import java.util.List;

@Data
public class QuotaMetadataExtendedRequest {
    private String clusterId;
    private List<QuotaEntry> quotaEntries;
}
