package main

import (
	"fmt"
	"redis/redisMQ"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")

	if err != nil {
		panic(err)
	}
	//example
	bufferedQueueArray := []string{"bufferQueue", "Fds"}
	queueArray := []string{"queue", "Fds"}

	redisMQ.RedisMQInit(bufferedQueueArray, queueArray)

	redisMQ.AddConsumer(1, "queue", "consumer1")
	redisMQ.AddConsumer(1, "queue", "consumer2")
	redisMQ.AddConsumer(2, "bufferQueue", "bufferConsumer1")

	redisMQ.QueuePush(1, "queue", "테스트일번")
	redisMQ.QueuePush(1, "queue", "테스트이번")
	_, result1 := redisMQ.QueuePop(1, "queue", "consumer1")

	fmt.Printf("Queue 결과 : %s\n", result1)

	_, result2 := redisMQ.QueuePop(1, "queue", "consumer2")

	fmt.Printf("Queue 결과 : %s\n", result2)

	redisMQ.BufferedQueueFlush(bufferedQueueArray)
	redisMQ.QueuePush(2, "bufferQueue", "테스트삼번")
	redisMQ.QueuePush(2, "bufferQueue", "테스트사번")

	_, data2 := redisMQ.QueuePop(2, "bufferQueue", "bufferConsumer1")
	fmt.Println(data2)

	_, data3 := redisMQ.QueuePop(2, "bufferQueue", "bufferConsumer1")
	fmt.Println(data3)

	redisMQ.QueuePush(2, "bufferQueue", "버퍼테스트1번")
	redisMQ.QueuePush(2, "bufferQueue", "버퍼테스트2번")
	redisMQ.QueuePush(2, "bufferQueue", "버퍼테스트3번")
	redisMQ.QueuePush(2, "bufferQueue", "버퍼테스트4번")

	fmt.Println("----------------------")
	_, data4 := redisMQ.BufferedQueueMultiPop("bufferQueue", "bufferConsumer1", 4)
	fmt.Println(data4)

	redisMQ.QueuePush(2, "bufferQueue", "버퍼테스트1번")
	redisMQ.QueuePush(2, "bufferQueue", "버퍼테스트2번")
	redisMQ.QueuePush(2, "bufferQueue", "버퍼테스트3번")
	redisMQ.QueuePush(2, "bufferQueue", "버퍼테스트4번")

	fmt.Println("----------------------")
	_, data5 := redisMQ.BufferedQueueMultiPop("bufferQueue", "bufferConsumer1", 4)
	fmt.Println(data5)

}
