package com.dust.corp.kafka.sample.entity.repository;

import com.dust.corp.kafka.sample.entity.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, Long> {
}
