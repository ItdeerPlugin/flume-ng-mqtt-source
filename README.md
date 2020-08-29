flume-ng-mqtt-source
================

本项目是对Apache Flume Source的扩展，支持MQTT服务器为数据源，基于Flume1.6.0版本扩展

编译打包
----------

```shell
$ mvn clean package
```

部署
----------

> 复制flume-ng-mqtt-source-<version>.jar到flume插件目录

> lib目录放自定义组件的jar包 libext目录下放自定义组件的依赖包

```shell
  $ mkdir -p $FLUME_HOME/plugins.d/mqtt-source/lib 
  $ mkdir -p $FLUME_HOME/plugins.d/mqtt-source/libext
  $ cp flume-ng-mqtt-source-<version>.jar $FLUME_HOME/plugins.d/mqtt-source/lib/
  $ cp org.eclipse.paho.client.mqttv3-1.2.4.jar $FLUME_HOME/plugins.d/mqtt-source/libext/
```

Source说明
----------

属性名称 | 是否必须 | 默认值 | 说明 | 示例
-- | -- | -- | -- | -- |
<b>type</b> | 是 |  | 自定义Source类型 |  org.apache.flume.source.mqtt.MqttSource
<b>host</b> | 是 |  | MQTT主机 |  tcp://127.0.0.1:1883
<b>topic</b> | 是 |  | 订阅主题 |  demo
qos | 否 | 1 | 确保消息被传到，可能会传多次 |  1
batchSize | 否 | 100 | 批大小 |  100
cleanSession | 否 | false | 设置是否清空session  |  false
connectionTimeout | 否 | 30 | 设置连接超时时间 |  30
keepAliveInterval | 否 | 60 | 设置会话心跳时间 |  60
username | 否 |   | 连接用户名 |  demo
password | 否 |   | 连接密码 |  demo
retryConnection | 否 | false  | 是否重连Mqtt服务器 | true

配置示例
--------------------

```properties
# 配置文件名称为 mqtt.conf
# 指定Source的类型

agent.sources = mqtt
agent.channels = memory-channel
agent.sinks = logger

agent.sources.mqtt.type = org.apache.flume.source.mqtt.MqttSource
agent.sources.mqtt.host = tcp://127.0.0.1:1883
agent.sources.mqtt.topic = demo
agent.sources.mqtt.qos = 1
agent.sources.mqtt.batchSize = 1000
agent.sources.mqtt.cleanSession = true
agent.sources.mqtt.connectionTimeout = 10
agent.sources.mqtt.keepAliveInterval = 100
agent.sources.mqtt.username = demo
agent.sources.mqtt.password = demo
agent.sources.mqtt.retryConnection = true

agent.channels.memory-channel.type = memory

agent.sinks.logger.type = logger

agent.sources.mqtt.channels = memory-channel
agent.sinks.logger.channel = memory-channel
```

启动任务
--------------------

```shell
flume-ng agent --name mqtt --conf $FLUME_HOME/conf --conf-file /opt/mqtt.conf -Dflume.root.logger=INFO,console

--name      : 指定的任务名称，和配置中的名称一致
--conf      : flume的配置
--conf-file : 自定义的配置文件
```