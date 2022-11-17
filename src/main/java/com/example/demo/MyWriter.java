package com.example.demo;

import org.springframework.batch.item.database.JpaItemWriter;

import java.util.List;

public class MyWriter extends JpaItemWriter<DemoEntity> {

    private boolean didThrowOnce = false;

    @Override
    public void write(List<? extends DemoEntity> items) {

        if (!didThrowOnce) {
            didThrowOnce = true;
            throw new RuntimeException();
        }

        super.write(items);
    }
}
