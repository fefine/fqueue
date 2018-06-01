package main

import (
	"context"
    . "google.golang.org/grpc/examples/demo/simple_rpc"
	"time"
	"fmt"
	"net"
	"google.golang.org/grpc"
	"log"
	"io"
)

type chatServer struct {
	Users []int32
}

func NewChatServer() *chatServer {
	return &chatServer{
		Users: make([]int32, 0, 10),
	}
}

func (cs *chatServer) Send(ctx context.Context, msg *Msg) (resp *MsgResp, err error) {
	log.Printf("recv: %v\n", msg)
	switch msg.Type {
	case MessageType_GROUP:
		log.Printf("[%v] %d send to group[%d], msg: %s",
			time.Unix(msg.Time, 0), msg.Fid, msg.Tid, msg.Msg)
	case MessageType_PRIMARY:
		log.Printf("[%v] %d send to group[%d], msg: %s",
			time.Unix(msg.Time, 0), msg.Fid, msg.Tid, msg.Msg)
	}
	return &MsgResp{
		Type: RespType_OK,
		Resp: fmt.Sprintf("hello %d", msg.Fid),
	}, nil
}

// 接收stream
func (cs *chatServer) SendStream(stream ChatServer_SendStreamServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			stream.SendAndClose(&MsgResp{
				Type:RespType_OK,
				Resp: "sendStream",
			})
			return nil
		}
		if err != nil {
			log.Fatalln(err)
			return err
		}
		log.Println(msg)
	}
}

// 返回 stream
func (cs *chatServer) RecvStream(msg *Msg, stream ChatServer_RecvStreamServer) error {
	log.Printf("-------\nrecv: %v\n", msg)
	for i := 0; i < 5; i++ {
		resp := &MsgResp{
			Type: RespType_OK,
			Resp: fmt.Sprintf("hello %d", i),
		}
		log.Println(resp)
		if err := stream.Send(resp); err != nil {
			log.Fatalln(err)
			return err
		}
	}
	log.Println("-----recvStream------")
	return nil
}

// 接收和返回stream
func (cs *chatServer) AllStream(stream ChatServer_AllStreamServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Println("recv done")
			return nil
		}
		if err != nil {
			log.Fatalln(err)
			return err
		}

		log.Println(msg)

		stream.Send(&MsgResp{
			Type:RespType_OK,
			Resp: fmt.Sprintf("resp to %v", msg.Msg),
		})
	}
}

func main() {
	lis, err := net.Listen("tcp", ":8090")
	if err != nil {
		log.Printf("listen error, %v\n", err)
	}
	grpcServer := grpc.NewServer()
	RegisterChatServerServer(grpcServer, NewChatServer())
	log.Println("start server")
	grpcServer.Serve(lis)
}