package io.confluent.migrationutility.model.quota;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.HashSet;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QuotaResponse {
  private List<QuotaEntry> sourceQuotas;
  private Integer sourceQuotaCount;
  private List<QuotaEntry> destinationQuotas;
  private Integer destinationQuotaCount;
  private Boolean srcSubsetExistsInDest = null;

  public QuotaResponse(final List<QuotaEntry> sourceQuotas) {
    this.sourceQuotas = sourceQuotas;
    this.sourceQuotaCount = sourceQuotas.size();
  }

  public QuotaResponse(final List<QuotaEntry> sourceQuotas, final List<QuotaEntry> destinationQuotas) {
    this.sourceQuotas = sourceQuotas;
    this.sourceQuotaCount = sourceQuotas.size();
    this.destinationQuotas = destinationQuotas;
    this.destinationQuotaCount = destinationQuotas.size();
    this.srcSubsetExistsInDest = new HashSet<>(destinationQuotas).containsAll(sourceQuotas);
  }

}
