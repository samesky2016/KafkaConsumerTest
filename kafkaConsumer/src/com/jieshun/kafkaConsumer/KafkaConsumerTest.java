package com.jieshun.kafkaConsumer;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerTest
{
  private ConsumerConnector consumer;
  private KafkaConsumer<String, String> kafkaConsumer;
  private String topic = "mb.park.in";
  private static long requestCount = 0L;
  private static Properties props;
  
  public KafkaConsumerTest()
  {
    props = new Properties();
    try
    {
      InputStream in = new BufferedInputStream(new FileInputStream(System.getProperty("user.dir") + File.separator + "app.properties"));
      props.load(in);
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    if ("".equals(props.getProperty("topic"))) {
      System.out.println("启用默认topic！" + this.topic);
    } else {
      this.topic = props.getProperty("topic");
    }
    this.consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
  }
  
  private static ConsumerConfig createConsumerConfig()
  {
    props.put("zookeeper.connect", props.getProperty("zookeeper.connect"));
    props.put("group.id", props.getProperty("group.id"));
    props.put("zookeeper.session.timeout.ms", "5000");
    props.put("auto.commit.interval.ms", "1000");
    props.put("rebalance.backoff.ms", "3000");
    props.put("rebalance.max.retries", "50");
    props.put("max.poll.records", "1000");
    props.put("max.poll.interval.ms", "60000");
    props.put("auto.offset.reset", "smallest");
    return new ConsumerConfig(props);
  }
  
  public void startConsume()
  {
    System.out.println("开始消费,并启动TPS实时统计线程......");
    
    new Thread()
    {
      public void run()
      {
        try
        {
          for (;;)
          {
            long startCounter = KafkaConsumerTest.requestCount;
            Thread.sleep(10000L);
            System.out.println("消费者，实时TPS---------------------->>：" + (KafkaConsumerTest.requestCount - startCounter) / 10L + "次/s");
          }
        }
        catch (InterruptedException e)
        {
          e.printStackTrace();
        }
      }
    }.start();
    Map<String, Integer> topicMap = new HashMap();
    ExecutorService threadPool = Executors.newFixedThreadPool(Integer.valueOf(props.getProperty("threadCount")).intValue());
    
    topicMap.put(this.topic, new Integer(Integer.valueOf(props.getProperty("threadCount")).intValue()));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = this.consumer.createMessageStreams(topicMap);
    List<KafkaStream<byte[], byte[]>> streamList = (List)consumerStreamsMap.get(this.topic);
    System.out.println("streamList size is : " + streamList.size());
    int counter = 1;
    for (KafkaStream<byte[], byte[]> stream : streamList) {
      try
      {
        threadPool.submit(new Task("consumer_" + counter++, stream));
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
  }
  
  static class Task
    implements Runnable
  {
    private String taskName;
    private KafkaStream<byte[], byte[]> stream;
    
    public Task(String taskName, KafkaStream<byte[], byte[]> stream)
    {
      this.taskName = taskName;
      this.stream = stream;
    }
    
    public void run()
    {
      System.out.println("task " + this.taskName + " is doing...");
      ConsumerIterator<byte[], byte[]> it = this.stream.iterator();
      while (it.hasNext())
      {
        MessageAndMetadata<byte[], byte[]> mes = it.next();
        

        KafkaConsumerTest.requestCount += 1L;
        System.out.println("接收数据条数----->> " + KafkaConsumerTest.requestCount + "    接收数据内容----->>" + new String((byte[])mes.message()));
      }
    }
  }
  
  public static void main(String[] args)
  {
	  KafkaConsumerTest consumer = new KafkaConsumerTest();
	  
	  consumer.startConsume();
  }
}
