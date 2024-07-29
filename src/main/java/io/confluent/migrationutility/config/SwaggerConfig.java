package io.confluent.migrationutility.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {

  @Bean
  public OpenAPI customOpenAPI() {
    return new OpenAPI()
            .info(new Info()
                    .title("Migration_Utility API")
                    .version("1.0.0")
                    .description("API capable of extracting metadata (cg, topics, acls, quotas) from source Kafka clusters and applying to destination Kafka clusters.")
            );
  }
}
