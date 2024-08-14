package model

type ConsumerMapKey struct {
	QueueName    string
	ConsumerName string
}

type RedisConfig struct {
	Host     string
	Port     string
	Password string
	RedisDB  int
}
