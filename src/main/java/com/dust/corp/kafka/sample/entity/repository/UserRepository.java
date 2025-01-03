package com.dust.corp.kafka.sample.entity.repository;

import com.dust.corp.kafka.sample.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
}
