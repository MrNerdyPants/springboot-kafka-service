package com.dust.corp.kafka.sample.service;

import com.dust.corp.kafka.sample.Charge;
import com.dust.corp.kafka.sample.ChargeType;
import com.dust.corp.kafka.sample.entity.Transaction;
import com.dust.corp.kafka.sample.entity.User;
import com.dust.corp.kafka.sample.entity.repository.TransactionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TransactionService {
    @Autowired
    private TransactionRepository transactionRepository;

    void chargeTransaction(User user, Charge charge) {
        Transaction transaction = new Transaction();

        transaction.setAmount(charge.getAmount());
        ChargeType chargeType = ChargeType.values()[charge.getChargeType()];
        transaction.setTransactionType(chargeType.name());

        Long newBalance = user.getBalance();
        if (chargeType.equals(ChargeType.CREDIT)) {
            newBalance -= charge.getAmount();
        } else {
            newBalance += charge.getAmount();
        }

        user.setBalance(newBalance);

        transaction.setUser(user);

        save(transaction);
    }

    Transaction save(Transaction transaction) {
        return transactionRepository.save(transaction);
    }
}
