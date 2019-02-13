# kafka_practice
基础
简介
是什么
kafka是一个消息中间件，具有分布式、可分区、可备份的日志服务，它使用独特的设计实现了一个消息系统的功能。

设计架构             


broker向zookeeper注册，通过zk选举leader，同时当consumer group发生变化时进行rebalance

producer向zk发送请求，拿到broker然后基于topic发送一个主题message，producer到broker是push

消费指定topic的一组consumer pull到消息，均衡消费

组成部分
Topic：特指Kafka处理的消息源的不同分类，其实也可以理解为对不同消息源的区分的一个标识；
Partition：Topic物理上的分组，一个topic可以设置为多个partition，每个partition都是一个有序的队列，partition中的每条消息都会被分配一个有序的id（offset）；

Message：消息，是通信的基本单位，每个producer可以向一个topic（主题）发送一些消息；

Producers：消息和数据生产者，向Kafka的一个topic发送消息的过程叫做producers（producer可以选择向topic哪一个partition发送数据）;

Consumers：消息和数据消费者，接收topics并处理其发布的消息的过程叫做consumer，同一个topic的数据可以被多个consumer接收；

Producers：消息和数据生产者，向Kafka的一个topic发送消息的过程叫做producers（producer可以选择向topic哪一个partition发送数据）;

Broker：缓存代理，Kafka集群中的一台或多台服务器统称为broker。

作用
系统间解耦、缓存实时数据用于离线处理等



API使用
1、内置的sacla版本客户端

maven依赖

<dependency>
   <groupId>org.apache.kafka</groupId>
   <artifactId>kafka_2.11</artifactId>
   <version>0.10.2.1</version>
</dependency>
2、新版本kafka客户端 

 maven 依赖 

<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>0.10.1.0</version>
</dependency>
 

3、spring集成客户端

maven依赖

<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
    <version>1.1.1.RELEASE</version>
</dependency>
生产者spring配置

<!--配置kafka参数-->
<bean id="agreementProperties" class="java.util.HashMap">
    <constructor-arg>
        <map>
            <entry key="bootstrap.servers" value="${kafka.server.port}"/>
            <entry key="retries" value="0"/>
            <entry key="batch.size" value="16384"/>
            <entry key="linger.ms" value="1"/>
            <entry key="request.timeout.ms" value="1000"/>
            <entry key="buffer.memory" value="33554432"/>
            <entry key="key.serializer" value="org.apache.kafka.common.serialization.IntegerSerializer"/>
            <entry key="value.serializer" value="org.apache.kafka.common.serialization.StringSerializer"/>
        </map>
    </constructor-arg>
</bean>
  
<!--创建bean工厂-->
<bean id="agreementFactory" class="org.springframework.kafka.core.DefaultKafkaProducerFactory">
    <constructor-arg ref="agreementProperties"/>
</bean>
 
<!--生成kafka操作客户端-->
<bean id="agreementKafka" class="org.springframework.kafka.core.KafkaTemplate">
    <constructor-arg ref="agreementFactory"/>
    <constructor-arg name="autoFlush" value="true"/>
</bean>
生产者代码：

@Service("kafkaProducerService")
public class KafkaProducerServiceImpl implements KafkaProducerService {
 
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerServiceImpl.class);
 
    @Resource(name = "agreementKafka")
    private KafkaTemplate<Integer, String> agreementKafka;
 
   @Override
    public void createContractKfkMsg(String topic, String msg) {
    logger.info("贷中合同创建进件010协议");
    long startTime = System.currentTimeMillis();
    //发送kafka消息
    ListenableFuture<SendResult<Integer, String>> listenableCallBack = agreementKafka.send(topic, msg);
    SuccessCallback<SendResult<Integer, String>> successCallback = (SendResult<Integer, String> result) -> {
        if (result != null && result.getRecordMetadata() != null) {
            logger.info(
                    "send the msg successfully, topic->{}, partition->{}, offset->{}, " +
                            "timestamp->{},serializedKeySize->{},serializedValueSize->{}",
                    topic, result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    result.getRecordMetadata().timestamp(), result.getRecordMetadata().serializedKeySize(),
                    result.getRecordMetadata().serializedValueSize());
        }
    };
    FailureCallback failureCallback = (Throwable ex) -> logger.info("[topic = {}, msg = {}], onFailure:", topic, msg
            , ex);
 
    listenableCallBack.addCallback(successCallback, failureCallback);
    logger.info("kafka producer send msg ,time is {}", System.currentTimeMillis() - startTime);
   }
}

消费者spring配置
 

<bean id = "agreementActivatedConsumerProperties" class="java.util.HashMap">
    <constructor-arg>
        <map>
            <entry key="zookeeper.connect" value="${zookeeper.connect}"/>
            <entry key="bootstrap.servers" value="${kafka.server.port}"/>
            <entry key="group.id" value="lease.templar.agreement.activated.group" />
            <entry key="enable.auto.commit" value="true" />
            <entry key="auto.commit.interval.ms" value="1000" />
            <entry key="session.timeout.ms" value="15000" />
            <entry key="key.deserializer" value="org.apache.kafka.common.serialization.StringDeserializer" />
            <entry key="value.deserializer" value="org.apache.kafka.common.serialization.StringDeserializer" />
        </map>
    </constructor-arg>
</bean>
<bean id="agreementActivatedConsumerFactory" class="org.springframework.kafka.core.DefaultKafkaConsumerFactory">
    <constructor-arg>
        <ref bean="agreementActivatedConsumerProperties"/>
    </constructor-arg>
</bean>
<bean id="agreementContainerProperties" class="org.springframework.kafka.listener.config.ContainerProperties">
    <constructor-arg value="${lease.agreement.activated.topic}" />
    <property name="messageListener" ref="agreementActivatedListenService" />
</bean>
<bean id="agreementListenerContainer" class="org.springframework.kafka.listener.KafkaMessageListenerContainer"
      init-method="doStart">
    <constructor-arg ref="agreementActivatedConsumerFactory" />
    <constructor-arg ref="agreementContainerProperties" />
</bean>
 

 消费者代码
 

@Service("agreementActivatedListenService")
@Slf4j
public class AgreementActivatedConsumerService implements MessageListener<Integer, String>{
    public static  final String AGREEMENT_REFUSE_PREFIX = "AGREEMENT_SERIAL_NO_";
 
    @Value("${lease.agereement.activated.state.callback.url}")
    private String callbackUrl;
 
    @Resource
    private PropertiesResource propertiesResource;
 
    @Override
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
        JSONObject jsonObj = JSONObject.parseObject(consumerRecord.value());
        if(jsonObj != null){
            log.info("消费到010协议的消息 {}", jsonObj.toJSONString());
            AgreementActivateMesssageBo agreementActivateMesssageBo = jsonObj.toJavaObject(AgreementActivateMesssageBo.class);
            String serialNo;
            if((serialNo = agreementActivateMesssageBo.getSerialNo()) != null){
                RedisTemplate redisTemplate = SpringContextUtil.getBean("redisTemplate", RedisTemplate.class);
                String value = (String)redisTemplate.opsForValue().get(AGREEMENT_REFUSE_PREFIX + serialNo);
                log.info("redis get value = {}" ,value);
                if(serialNo.equals(value)){ //匹配有效单
                    //回调阿里云服务 TODO 处理下
                    try{
                        Map<String, String> params = new HashMap<>();
                        params.put("data", DESedeHelper.encrypt(jsonObj.toJSONString(), propertiesResource.securityClfLeaseCommonSecretKey));
                        String res = HttpUtils.doPost(callbackUrl, params, "utf-8");
                        log.info("回调结果==" + res);
                    }catch (Exception ex){
                        log.error("callback error", ex);
                    }
                }
            }
        }
    }
}
 

