package com.example.demo;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.AbstractStep;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
@EnableBatchProcessing
public class DemoApplication {

    private static List<String> itemNamesFromSkipListener = new ArrayList<>();
    private static List<String> itemLifecycles = new ArrayList<>();

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
        System.out.println("itemNamesFromSkipListener:");
        System.out.println(String.join("\n", itemNamesFromSkipListener));
        System.out.println();
        System.out.println("itemLifecycles:");
        System.out.println(String.join("\n", itemLifecycles));
    }

    @Autowired
    private DataSource dataSource;

    @PostConstruct
    public void postConstruct() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.update("CREATE TABLE IF NOT EXISTS demo (field INTEGER, UNIQUE(field))");
    }

    @Bean
    public Job job(JdbcTemplate jdbcTemplate, JobRepository jobRepository, PlatformTransactionManager platformTransactionManager) {
        TaskletStep step1 = new StepBuilder("throwing runtime exception step")
                .transactionManager(platformTransactionManager)
                .repository(jobRepository)
                .<String, String>chunk(1)
                .reader(new CountingReader("runtime", 2))
                .writer(new ThrowingWriter())
                .faultTolerant()
                .skipPolicy(new AlwaysSkipPolicy())
//                .skip(Throwable.class)
//                .skipLimit(Integer.MAX_VALUE)
                .listener(new MyWriteListener())
                .listener(new MySkipListener())
                .build();

        TaskletStep step2 = new StepBuilder("throwing contraint exception on commit step")
                .transactionManager(platformTransactionManager)
                .repository(jobRepository)
                .<String, String>chunk(1)
                .reader(new CountingReader("constraint", 2))
                .writer(new ConstraintViolationWriter(jdbcTemplate))
                .faultTolerant()
                .skipPolicy(new AlwaysSkipPolicy())
//                .skip(Throwable.class)
//                .skipLimit(Integer.MAX_VALUE)
                .listener(new MySkipListener())
                .listener(new MyWriteListener())
                .build();

        Job job = new JobBuilder("demo")
                .repository(jobRepository)
                .start(step1)
                .next(step2)
                .next(new AbstractStep() {
                    @Override
                    protected void doExecute(StepExecution stepExecution) throws Exception {
                        System.out.println("hello world");
                    }
                })
                .build();
        return job;
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    public static class CountingReader implements ItemReader<String> {
        private int read = 0;

        private final String name;
        private final int itemCount;

        public CountingReader(String name, int itemCount) {
            this.name = name;
            this.itemCount = itemCount;
        }

        @Override
        public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
            if (read < itemCount) {
                read++;
                return name + "-item-" + read;
            } else {
                return null;
            }
        }
    }

    public static class ThrowingWriter implements ItemWriter<String> {

        @Override
        public void write(List<? extends String> items) throws Exception {
            if (!items.isEmpty()) {
                String message = "runtime exception for " + items.get(0);
                throw new RuntimeException(message);
            }
        }
    }

    public static class ConstraintViolationWriter implements ItemWriter<String> {

        private final JdbcTemplate jdbcTemplate;

        public ConstraintViolationWriter(JdbcTemplate jdbcTemplate) {
            this.jdbcTemplate = jdbcTemplate;
        }

        @Override
        public void write(List<? extends String> items) throws Exception {
            jdbcTemplate.update("INSERT INTO demo VALUES (1)");
        }
    }

    public static class AlwaysSkipPolicy implements SkipPolicy {
        @Override
        public boolean shouldSkip(Throwable t, int skipCount) throws SkipLimitExceededException {
            return true;
        }
    }

    public static class MySkipListener implements SkipListener<String, String> {

        @Override
        public void onSkipInRead(Throwable t) {
            throw new IllegalStateException("this is not intended");
        }

        @Override
        public void onSkipInWrite(String item, Throwable t) {
            itemNamesFromSkipListener.add(item);
        }

        @Override
        public void onSkipInProcess(String item, Throwable t) {
            throw new IllegalStateException("this is not intended");
        }
    }

    public static class MyWriteListener implements ItemWriteListener<String> {

        @Override
        public void beforeWrite(List<? extends String> items) {
            itemLifecycles.add("beforeWrite " + items.get(0));
        }

        @Override
        public void afterWrite(List<? extends String> items) {
            itemLifecycles.add("afterWrite " + items.get(0));
        }

        @Override
        public void onWriteError(Exception exception, List<? extends String> items) {
            itemLifecycles.add("onWriteError " + items.get(0));
        }
    }
}
