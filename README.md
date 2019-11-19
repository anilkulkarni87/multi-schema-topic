### Publish multiple avro schemas to same Kafka Topic
<p>
<a>
<img alt="GitHub last commit (branch)" src="https://img.shields.io/github/last-commit/anilkulkarni87/spark-apps/master">
</a>
  <a href="https://twitter.com/anilkulkarni" target="_blank">
    <img alt="Twitter: anilkulkarni" src="https://img.shields.io/twitter/follow/anilkulkarni.svg?style=social" />
  </a>
</p>
Sample to project to understand how to publish and consume messages of multiple schemas to same topic.

Steps to setup this project:
1) Use Docker to setup the kafka stack: <br>
```docker run --rm -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=127.0.0.1 landoop/fast-data-dev:latest```

2) Build the project gradle clean build  <br>
`gradle clean build`

3) Run the Generic Producer and Consumer

4) Run the Specific Producer and Consumer

#####TODO:
1) How to cast to right Object when consuming multiple schemas from same topic
2) Reusable KafkaConfig
