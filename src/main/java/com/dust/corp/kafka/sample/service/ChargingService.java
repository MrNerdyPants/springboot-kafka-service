package com.dust.corp.kafka.sample.service;

import com.dust.corp.kafka.sample.Charge;
import com.dust.corp.kafka.sample.ChargeType;
import com.dust.corp.kafka.sample.entity.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@Service
public class ChargingService {
    @Autowired
    UserRepository userRepository;

    public List<Charge> createChargeForAllUser() {
        return userRepository.findAll().stream().map(user -> Charge.builder()
                .userId(user.getId())
                .chargeType(getRandomChargeType().ordinal())
                .amount(getRandomCharge()).build())
                .collect(Collectors.toList());
    }

    public Long getRandomCharge() {
        return Long.valueOf(new Random().nextInt(10));
    }

    public ChargeType getRandomChargeType() {
        return ChargeType.class.getEnumConstants()[new Random().nextInt(ChargeType.values().length)];
    }



}
