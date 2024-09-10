package redisMQ

import (
	"os"
	"redis/redisMQ/model"
	"strconv"

	"github.com/adjust/redismq"
)

/*
 *  QueueType
 *  1.queue
 *  2.bufferedQueue
 */
var QueueType map[int]string
var QueueMap map[string]*redismq.Queue
var BufferedQueueMap map[string]*redismq.BufferedQueue
var ConsumerMap map[model.ConsumerMapKey]*redismq.Consumer
var RedisConfig *model.RedisConfig

// I have found anything over 200 as bufferSize not to increase performance any further.

const bufferSize = 200

func RedisMQInit(bufferedQueueName []string, queueName []string) {

	redisDB, err := strconv.Atoi(os.Getenv("REDIS_DB"))
	if err != nil {
		panic(err)
	}
	RedisConfig = &model.RedisConfig{os.Getenv("REDIS_HOST"),
		os.Getenv("REDIS_PORT"), os.Getenv("REDIS_PASSWORD"), redisDB}

	QueueType = make(map[int]string)
	QueueType[1] = "queue"
	QueueType[2] = "bufferedQueue"
	QueueMap = make(map[string]*redismq.Queue)
	BufferedQueueMap = make(map[string]*redismq.BufferedQueue)
	ConsumerMap = make(map[model.ConsumerMapKey]*redismq.Consumer)

	for _, name := range bufferedQueueName {
		queue := redismq.CreateBufferedQueue(RedisConfig.Host, RedisConfig.Port, RedisConfig.Password, int64(RedisConfig.RedisDB), name, bufferSize)
		queue.Start()
		BufferedQueueMap[name] = queue
	}

	for _, name := range queueName {
		queue := redismq.CreateQueue(RedisConfig.Host, RedisConfig.Port, RedisConfig.Password, int64(RedisConfig.RedisDB), name)
		QueueMap[name] = queue
	}

}

func QueuePush(queueType int, queueName string, payload string) bool {

	if !isExistQueue(queueType, queueName) {

		return false
	}
	/* QueueType : queue */
	if queueType == 1 {
		queue := QueueMap[queueName]
		err := queue.Put(payload)
		if err != nil {
			panic(err)
		}
	} else {
		/* QueueType : bufferedQueue */
		bufferedQueue := BufferedQueueMap[queueName]
		err := bufferedQueue.Put(payload)
		if err != nil {
			panic(err)
		}
	}
	return true
}

func QueuePop(queueType int, queueName string, consumerName string) (bool, string) {

	if !isExistQueue(queueType, queueName) {
		return false, "Queue not exist"
	}

	if !isExistConsumerMap(queueType, consumerName) {
		return false, "ConsumerMap not exist"
	}

	consumer := ConsumerMap[model.ConsumerMapKey{QueueType[queueType], consumerName}]

	/* unaked 처리 어떻게? */
	if consumer.HasUnacked() {
		data, err := consumer.GetUnacked()
		if err != nil {
			panic(err)
		}
		err = data.Ack()
		if err != nil {
			panic(err)
		}
		return true, data.Payload
	}

	data, err := consumer.Get()

	if err != nil {
		panic(err)

	}
	err = data.Ack()
	if err != nil {
		panic(err)
	}

	return true, data.Payload
}

func BufferedQueueMultiPop(queueName string, consumerName string, size int) (bool, []string) {

	if !isExistQueue(2, queueName) {
		return false, nil
	}
	if !isExistConsumerMap(2, consumerName) {
		return false, nil
	}
	if size > 20 {
		size = 20
	}

	consumer := ConsumerMap[model.ConsumerMapKey{QueueType[2], consumerName}]
	packages, err := consumer.MultiGet(size)
	if err != nil {
		panic(err)
	}

	returnData := []string{}

	for i := range packages {
		returnData = append(returnData, packages[i].Payload)
	}

	packages[len(packages)-1].MultiAck()

	return true, returnData
}

func AddConsumer(queueType int, queueName string, consumerName string) bool {

	if !isExistQueue(queueType, queueName) {
		return false
	}
	/* QueueType : queue */
	if queueType == 1 {
		queue := QueueMap[queueName]

		consumer, err := queue.AddConsumer(consumerName)
		if err != nil {

			panic(err)
		}
		ConsumerMap[model.ConsumerMapKey{QueueType[queueType], consumerName}] = consumer
	} else {
		/* QueueType : bufferedQueue */
		bufferedQueue := BufferedQueueMap[queueName]
		consumer, err := bufferedQueue.AddConsumer(consumerName)
		if err != nil {
			panic(err)
		}
		ConsumerMap[model.ConsumerMapKey{QueueType[queueType], consumerName}] = consumer

	}

	return true
}

func BufferedQueueFlush(queueNames []string) {
	for _, queueName := range queueNames {
		bufferedQueue := BufferedQueueMap[queueName]
		bufferedQueue.FlushBuffer()
	}
}

func isExistConsumerMap(queueType int, consumerName string) bool {
	if _, exists := ConsumerMap[model.ConsumerMapKey{QueueType[queueType], consumerName}]; exists {
		return true
	}
	return false
}

func isExistQueue(queueType int, queueName string) bool {

	if queueType == 1 {
		if _, exists := QueueMap[queueName]; exists {
			return true
		}
		return false

	} else {
		if _, exists := BufferedQueueMap[queueName]; exists {
			return true
		}
		return false

	}

}
