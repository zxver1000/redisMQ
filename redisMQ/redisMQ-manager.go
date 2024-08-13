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
var QueueList map[string]*redismq.Queue
var BufferedQueueList map[string]*redismq.BufferedQueue
var Consumer map[model.Pair]*redismq.Consumer
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
	QueueList = make(map[string]*redismq.Queue)
	BufferedQueueList = make(map[string]*redismq.BufferedQueue)
	Consumer = make(map[model.Pair]*redismq.Consumer)

	for _, name := range bufferedQueueName {
		queue := redismq.CreateBufferedQueue(RedisConfig.Host, RedisConfig.Port, RedisConfig.Password, int64(RedisConfig.RedisDB), name, bufferSize)
		queue.Start()
		BufferedQueueList[name] = queue
	}

	for _, name := range queueName {
		queue := redismq.CreateQueue(RedisConfig.Host, RedisConfig.Port, RedisConfig.Password, int64(RedisConfig.RedisDB), name)
		QueueList[name] = queue
	}

}

func QueuePush(queueType int, queueName string, payload string) bool {

	if !isExistQueue(queueType, queueName) {

		return false
	}
	/* QueueType : queue */
	if queueType == 1 {
		queue := QueueList[queueName]
		err := queue.Put(payload)
		if err != nil {
			panic(err)
		}
	} else {
		/* QueueType : bufferedQueue */
		bufferedQueue := BufferedQueueList[queueName]
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

	if !isExistConsumer(queueType, consumerName) {
		return false, "Consumer not exist"
	}

	consumer := Consumer[model.Pair{QueueType[queueType], consumerName}]

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
	if !isExistConsumer(2, consumerName) {
		return false, nil
	}
	if size > 20 {
		size = 20
	}

	consumer := Consumer[model.Pair{QueueType[2], consumerName}]
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
		queue := QueueList[queueName]

		consumer, err := queue.AddConsumer(consumerName)
		if err != nil {

			panic(err)
		}
		Consumer[model.Pair{QueueType[queueType], consumerName}] = consumer
	} else {
		/* QueueType : bufferedQueue */
		bufferedQueue := BufferedQueueList[queueName]
		consumer, err := bufferedQueue.AddConsumer(consumerName)
		if err != nil {
			panic(err)
		}
		Consumer[model.Pair{QueueType[queueType], consumerName}] = consumer

	}

	return true
}

func BufferedQueueFlush(queueNames []string) {
	for _, queueName := range queueNames {
		bufferedQueue := BufferedQueueList[queueName]
		bufferedQueue.FlushBuffer()
	}
}

func isExistConsumer(queueType int, consumerName string) bool {
	if _, exists := Consumer[model.Pair{QueueType[queueType], consumerName}]; exists {
		return true
	}
	return false
}

func isExistQueue(queueType int, queueName string) bool {

	if queueType == 1 {
		if _, exists := QueueList[queueName]; exists {
			return true
		}
		return false

	} else {
		if _, exists := BufferedQueueList[queueName]; exists {
			return true
		}
		return false

	}

}
