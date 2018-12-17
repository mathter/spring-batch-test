package biz.ostw.test.spring.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class Main {

	private static ApplicationContext ac;

	public static void main(String[] args) {

		ac = SpringApplication.run(Main.class, args);

	}

	@Scheduled(initialDelay = 1000, fixedRate = 2000)
	public void task() throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		JobLauncher launcher = ac.getBean(JobLauncher.class);
		Job job = (Job) ac.getBean("formatJob");

		launcher.run(job, new JobParameters());

	}
}
