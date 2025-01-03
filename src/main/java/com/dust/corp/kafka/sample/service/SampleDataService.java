package com.dust.corp.kafka.sample.service;

import com.dust.corp.kafka.sample.entity.User;
import com.dust.corp.kafka.sample.entity.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Service
public class SampleDataService {
    @Autowired
    private UserRepository userRepository;

    public void insertData() {
        if (userRepository.count() == 0) {
            for (int i = 0; i < 10; i++) {
                User user = new User();
                user.setBalance(100l);
                user.setName("User " + i);
                userRepository.save(user);
            }
        }
    }


    @EventListener
    public void onApplicationReady(ApplicationReadyEvent event) {
        insertData();
    }

}
