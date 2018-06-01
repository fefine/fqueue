package main

import (
	"log"
	. "google.golang.org/grpc/examples/demo/simple_rpc"
	"google.golang.org/grpc"
	"time"
	"context"
	"io"
)

func send(client ChatServerClient) {
	msg := &Msg{
		Fid: 1,
		Tid: 2,
		Msg: "sample msg",
		Type:MessageType_PRIMARY,
		Time: time.Now().Unix(),
	}
	log.Printf("send: %v\n", msg)
	resp, err := client.Send(context.Background(), msg)
	if err != nil {
		log.Printf("resp error\n")
	}
	log.Printf("recv: %v\n", resp)
}
func sendStream(client ChatServerClient) {
	stream, err := client.SendStream(context.Background())
	if err != nil {
		log.Fatalf("send stream: %v", err)
	}
	for i := 0; i < 5; i++ {
		msg := &Msg{
			Fid: 1 + int32(i * 10),
			Tid: 2 + int32(i * 10),
			Msg: "sample msg",
			Type:MessageType_PRIMARY,
			Time: time.Now().Unix(),
		}
		log.Printf("send: %v\n", msg)
		if err = stream.Send(msg); err != nil {
			log.Fatalf("send stream err: %v", err)
			return
		}

	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(resp)

}
func recvStream(client ChatServerClient) {
	msg := &Msg{
		Fid: 1,
		Tid: 2,
		Msg: "sample msg",
		Type:MessageType_PRIMARY,
		Time: time.Now().Unix(),
	}
	stream, err := client.RecvStream(context.Background(), msg)
	if err != nil {
		log.Fatalln(err)
	}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Println("recv done")
			return
		}
		if err != nil {
			log.Fatalln(err)
			return
		}

		log.Println(msg)
	}
}

func AllStream(client ChatServerClient) {
	stream, err := client.AllStream(context.Background())
	if err != nil {
		log.Fatalln(err)
		return
	}
	sig := make(chan int, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				log.Println("recv done")
				sig <- 1
				return
			}
			if err != nil {
				log.Fatalln(err)
				return
			}
			log.Println(msg)
		}
	}()
	for i := 0; i < 5; i++ {
		msg := &Msg{
			Fid: 1 + int32(i * 10),
			Tid: 2 + int32(i * 10),
			Msg: "sample msg",
			Type:MessageType_PRIMARY,
			Time: time.Now().Unix(),
		}
		log.Printf("send: %v\n", msg)
		if err = stream.Send(msg); err != nil {
			log.Fatalf("send stream err: %v", err)
			return
		}
		// 阻塞
		<- sig
	}

	if err = stream.CloseSend(); err != nil {
		log.Fatalf("send stream err: %v", err)
		return
	}
	<- sig
	log.Println("done")

}

/*
	RecvStream(ctx context.Context, in *Msg, opts ...grpc.CallOption) (ChatServer_RecvStreamClient, error)
	AllStream(ctx context.Context, opts ...grpc.CallOption) (ChatServer_AllStreamClient, error)

 */

func main() {
	conn, err := grpc.Dial("127.0.0.1:8090", grpc.WithInsecure())
	if err != nil {
		log.Println("dial error")
	}
	defer conn.Close()
	client := NewChatServerClient(conn)
	//send(client)
	//sendStream(client)
	//recvStream(client)
	AllStream(client)
}
