## spring-mq是什么
* spring-mq是一个以注解方式集成spring与rocketmq 或 spring与(阿里云)ons的框架

## spring-mq 实现了哪些功能
- 简化了MQ的使用,面向接口编程的方式使用MQ
- 封装了MQ的具体实现，可不更改业务代码地切换RocketMQ及ONS
- 兼容abtest的生产-灰度环境测试
- 兼容spring3.2.x 及 springboot1.4.5
- 支持rocketmq3.5.8 +

## 设计
![image](https://github.com/shook2012/spring-mq/raw/master/design.png)

## 示例
![image](https://github.com/shook2012/spring-mq/raw/master/demo.png)

## Future
![image](https://github.com/shook2012/spring-mq/raw/master/future.png)

本项目使用Apache开源协议
## License
* [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
