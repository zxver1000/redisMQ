package model

type Pair struct {
	QueueName    string
	ConsumerName string
}

type RedisConfig struct {
	Host     string
	Port     string
	Password string
	RedisDB  int
}
