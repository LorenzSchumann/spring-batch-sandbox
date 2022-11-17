package com.example.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.PostConstruct;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@SpringBootApplication
@EnableBatchProcessing
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Autowired
    private DataSource dataSource;

    @PostConstruct
    public void postConstruct() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.update("DROP TABLE IF EXISTS demo");
        jdbcTemplate.update("CREATE TABLE demo (id BIGSERIAL, unique_field TEXT, UNIQUE(unique_field))");
    }

    @Bean
    public Job job(JobRepository jobRepository,
                   PlatformTransactionManager platformTransactionManager,
                   MyWriter myWriter) {

        TaskletStep step1 = new StepBuilder("read/write demo entities")
                .transactionManager(platformTransactionManager)
                .repository(jobRepository)
                .<String, DemoEntity>chunk(1)
                .reader(new ListItemReader<>(List.of("1", "2", "3", "3", "4", "5")))
                .processor((Function<String, DemoEntity>) DemoEntity::new)
                .writer(myWriter)
                .faultTolerant()
                .skipPolicy((throwable, skipCount) -> true)
                .build();

        return new JobBuilder("demo")
                .repository(jobRepository)
                .start(step1)
                .listener(new JobExecutionListenerSupport() {
                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                        List<Map<String, Object>> data = jdbcTemplate.queryForList("select * from demo");
                        data.stream().forEach(System.out::println);
                    }
                })
                .build();
    }

    @Bean
    public MyWriter demoEntityWriter(EntityManagerFactory entityManagerFactory) {
        MyWriter myWriter = new MyWriter();
        myWriter.setEntityManagerFactory(entityManagerFactory);
        return myWriter;
    }
}
