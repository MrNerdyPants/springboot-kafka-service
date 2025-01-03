package com.dust.corp.kafka.sample;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Charge {
    Long userId;
    Long amount;
    int chargeType;
}
