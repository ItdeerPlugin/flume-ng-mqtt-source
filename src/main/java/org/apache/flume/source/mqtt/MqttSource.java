package org.apache.flume.source.mqtt;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Description : Mqtt Source Process
 * PackageName : org.apache.flume.source.mqtt
 * ProjectName : flume-parent
 * CreatorName : itdeer.cn
 * CreateTime : 2020/8/27/16:46
 */
public class MqttSource extends AbstractSource
        implements Configurable, PollableSource {

    private static final Logger log = LoggerFactory.getLogger(MqttSource.class);

    private String host;
    private String topic;
    private Integer qos;
    private Integer batchSize;

    private Boolean cleanSession;
    private Integer connectionTimeout;
    private Integer keepAliveInterval;
    private String username;
    private String password;
    private Boolean retryConnection;

    private String clientId = MqttSourceConstants.getUuid();

    private MemoryPersistence memoryPersistence = null;
    private MqttConnectOptions mqttConnectOptions = null;
    private MqttClient mqttClient = null;

    private Context context;
    private Event event;
    private final List<Event> eventList = new ArrayList<Event>();

    /**
     * Process business
     *
     * @return
     */
    @Override
    public Status process() {

        if (null != mqttClient && mqttClient.isConnected()) {
            if (null != topic && null != qos) {
                try {
                    mqttClient.subscribe(topic, qos);
                } catch (MqttException e) {
                    log.error("Subscription topic {} has an exception, the exception is: {}", topic, e.fillInStackTrace());
                }
            } else {
                throw new ConfigurationException("Mqtt host or topic config error.");
            }
            return Status.READY;
        } else {
            log.error("The MQTT connection is disconnected and reconnected ......");
            if (retryConnection) {
                reConnect();
                return Status.READY;
            }
            return Status.BACKOFF;
        }
    }

    /**
     * Mqtt configure
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        this.context = context;

        host = context.getString(MqttSourceConstants.HOST);
        if (host == null) {
            throw new ConfigurationException("Mqtt host must be specified.");
        }

        topic = context.getString(MqttSourceConstants.TOPIC);
        if (topic == null) {
            throw new ConfigurationException("Mqtt topic must be specified.");
        }

        qos = context.getInteger(MqttSourceConstants.QOS, MqttSourceConstants.DEFAULT_QOS);
        batchSize = context.getInteger(MqttSourceConstants.BATCH_SIZE);

        cleanSession = context.getBoolean(MqttSourceConstants.IF_SESSION_CLEAN, MqttSourceConstants.DEFAULT_IF_SESSION_CLEAN);
        connectionTimeout = context.getInteger(MqttSourceConstants.CONNECTION_TIMEOUT, MqttSourceConstants.DEFAULT_CONNECTION_TIMEOUT);
        keepAliveInterval = context.getInteger(MqttSourceConstants.KEEP_ALIVE_INTERVAL, MqttSourceConstants.DEFAULT_KEEP_ALIVE_INTERVAL);

        username = context.getString(MqttSourceConstants.USERNAME);
        password = context.getString(MqttSourceConstants.PASSWORD);

        retryConnection = context.getBoolean(MqttSourceConstants.RETRY_CONNECTION, MqttSourceConstants.DEFAULT_RETRY_CONNECTION);
    }

    /**
     * Start Job
     */
    @Override
    public synchronized void start() {
        log.info("Starting mqtt source job ......");

        mqttConnectOptions = new MqttConnectOptions();
        memoryPersistence = new MemoryPersistence();

        if (null != memoryPersistence && null != host) {
            try {
                mqttClient = new MqttClient(host, clientId, memoryPersistence);
            } catch (MqttException e) {
                throw new ConfigurationException("Mqtt host config error and error message : {}", e.fillInStackTrace());
            }
        }

        if (null != mqttConnectOptions) {

            mqttConnectOptions.setCleanSession(cleanSession);
            mqttConnectOptions.setConnectionTimeout(connectionTimeout);
            mqttConnectOptions.setKeepAliveInterval(keepAliveInterval);
            if (null != username) {
                mqttConnectOptions.setUserName(username);
            }
            if (null != password) {
                mqttConnectOptions.setPassword(password.toCharArray());
            }

            if (null != mqttClient && !mqttClient.isConnected()) {

                mqttClient.setCallback(new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        log.error("MqttClient disconnect call back retry connect......");
                        reConnect();
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) {
                        event = EventBuilder.withBody(message.getPayload());
                        if (null == batchSize) {
                            getChannelProcessor().processEvent(event);
                        } else if (eventList.size() < batchSize) {
                            eventList.add(event);
                        } else {
                            getChannelProcessor().processEventBatch(eventList);
                            eventList.clear();
                        }
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {

                    }
                });

                try {
                    mqttClient.connect();
                } catch (MqttException e) {
                    log.error("Get the MQTT connection exception, exception information is : {}", e.fillInStackTrace());
                    reConnect();
                }
            }
        }
    }

    public void reConnect() {

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            log.error("Retry Get the MQTT Thread sleep exception, exception information is : {}", e.fillInStackTrace());
        }

        if (null != mqttClient) {
            if (!mqttClient.isConnected()) {
                if (null != mqttConnectOptions) {
                    try {
                        mqttClient.connect(mqttConnectOptions);
                    } catch (MqttException e) {
                        log.error("Retry Get the MQTT connection exception, exception information is : {}", e.fillInStackTrace());
                    }
                }
            }
        } else {
            start();
        }
    }

    /**
     * close source
     */
    @Override
    public synchronized void stop() {
        if (mqttClient != null) {
            try {
                mqttClient.close();
            } catch (MqttException e) {
                log.error("mqttClient close an error occurs : {}", e.fillInStackTrace());
            }
        }

        if (mqttConnectOptions != null) {
            mqttConnectOptions = null;
        }

        if (memoryPersistence != null) {
            try {
                memoryPersistence.close();
            } catch (MqttPersistenceException e) {
                log.error("memoryPersistence close an error occurs : {}", e.fillInStackTrace());
            }
        }

        log.info("Mqtt Source {} stopped success.", getName());
        super.stop();
    }

}
