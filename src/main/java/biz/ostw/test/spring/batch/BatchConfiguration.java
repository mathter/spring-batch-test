package biz.ostw.test.spring.batch;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.repeat.exception.ExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableBatchProcessing
@Slf4j
public class BatchConfiguration {
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	public JobLauncher jobLauncher;

	@Autowired
	public JobRepository jobRepository;

	@Bean
	public JobExecutionDecider decider() {
		return new JobExecutionDecider() {

			@Override
			public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
				return FlowExecutionStatus.COMPLETED;
			}
		};
	}

	@Bean
	public Job formatJob() {
		JobBuilder jb = this.jobBuilderFactory.get("formaJob").incrementer(new RunIdIncrementer());
		JobFlowBuilder jfb = jb.flow(this.step0());

		jfb.from(this.step0()).next(this.decider());

		jfb.from(this.decider()).on("COMPLETED").to(this.stopStep());

		return jfb.build().build();
	}

	@Bean
	public Step stopStep() {
		return this.stepBuilderFactory.get("stopStep").<Object, Object>chunk(1).reader(new ItemReader<Object>() {
			@Override
			public Object read()
					throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
				return null;
			}
		}).writer(new ItemWriter<Object>() {

			@Override
			public void write(List<? extends Object> items) throws Exception {
			}
		}).build();
	}

	@Bean
	public Step step0() {
		return this.stepBuilderFactory.get("step0").<Date, String>chunk(10).reader(reader()).processor(this.processor())
				
				.writer(this.writer()).faultTolerant().allowStartIfComplete(true)
				.listener(new ChunkListener() {
					
					@Override
					public void beforeChunk(ChunkContext context) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void afterChunkError(ChunkContext context) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void afterChunk(ChunkContext context) {
						context.getStepContext().getStepExecution().getJobExecution().setExitStatus(ExitStatus.COMPLETED);
					}
				})
				.build();
	}

	@Bean
	public ItemProcessor<Date, String> processor() {
		return new ItemProcessor<Date, String>() {
			private DateFormat format = new SimpleDateFormat("yyyy MM dd HH:mm:ss.SSS");

			public String process(Date item) throws Exception {
				log.info("Process ...");
				return this.format.format(item);
			}
		};
	}

	@Bean
	public ItemWriter<String> writer() {
		return new ItemWriter<String>() {

			@Override
			public void write(List<? extends String> items) throws Exception {

				log.info("Write items:");

				for (String s : items) {
					log.info(s);
				}
			}
		};
	}

	@Bean
	public ItemReader<Date> reader() {
		return new ItemReader<Date>() {
			public Date read()
					throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
				log.info("Read ...");
				return new Date();
			}
		};
	}
}
