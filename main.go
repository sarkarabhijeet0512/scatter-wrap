package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	topic         = "message-log"
	brokerAddress = "localhost:9092"
)

var client *mongo.Client

// MessageLog is an Exporrtable type struct
type MessageLog struct {
	ID    primitive.ObjectID `json:"_id,omitempty" bson:"_id,omitempty"`
	Name  string             `json:"name,omitempty" bson:"name,omitempty"`
	Email string             `json:"email,omitempty" bson:"email,omitempty"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Connected to MongoDB!")
	fmt.Println("Connected to server 127.0.0.1:8080")
	// ctx = context.Background()
	go produce(ctx)
	consume(ctx)

}
func ProduceStruct() interface{} {
	var name string
	var email string

	fmt.Println("Enter name: ")
	fmt.Scan(&name)
	fmt.Println("Enter email: ")
	fmt.Scan(&email)

	message := MessageLog{Name: name, Email: email}
	return message
}
func produce(ctx context.Context) {

	data := ProduceStruct()
	bytedata, err := json.Marshal(data)
	if err != nil {
		fmt.Println("cannot parse")
	}
	// fmt.Println(bytedata)

	// initialize a counter
	// i := 0

	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})

	// for {
	// each kafka message has a key and value. The key is used
	// to decide which partition (and consequently, which broker)
	// the message gets published on

	err = w.WriteMessages(ctx, kafka.Message{
		Key: []byte(strconv.Itoa(1)),
		// create an arbitrary message payload for the value
		Value: []byte(bytedata),
	})
	if err != nil {
		panic("could not write message " + err.Error())
	}

	// log a confirmation once the message is written
	// sendDataToMongoDB()
	fmt.Println("writes:")
	// i++
	// sleep for a second
	time.Sleep(time.Second)
	// }
}
func consume(ctx context.Context) {
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "my-group",
		// MinBytes:    5, // kafka library requies to set the maxBytes if minByte is used
		// MaxBytes:    1e6,
		// MaxWait:     3 * time.Second, //wait for at most 3 seconds before receiving new data
		// StartOffset: kafka.FirstOffset,
		StartOffset: kafka.LastOffset, // if you set it to `kafka.LastOffset` it will only consume new messages
	})
	// the `ReadMessage` method blocks until we receive the next event
	msg, err := r.ReadMessage(ctx)
	if err != nil {
		panic("could not read message " + err.Error())
	}
	// after receiving the message, log its value
	sendDataToMongoDB(msg.Value)
	// fmt.Println("received: ", string(msg.Value))
}

// DbConnect function is used to secure Connection to the database
func sendDataToMongoDB(message []byte) error {
	var log MessageLog
	fmt.Println(string(message))
	err := json.Unmarshal(message, &log)
	if err != nil {
		fmt.Println("unable to unmarshal the file")
		return err
	}
	collection := client.Database("messagelog").Collection("message")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = collection.InsertOne(ctx, log)
	if err != nil {
		fmt.Println("Error is", err)
		return err
	}
	fmt.Println("Sucessfully Inserted message")
	//Return success without any error.
	return err
}
