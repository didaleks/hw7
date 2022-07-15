package main

import (
   "context"
   "fmt"
   "log"
   "net"

   "google.golang.org/grpc"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
func main() {
   // запуск сервиса
   address := "127.0.0.1:8082"
   StartMyMicroservice(address)
}

func StartMyMicroservice(address string) {
   listener, err := net.Listen("tcp", address)
   if err != nil {
      log.Fatalln("can't listen port", err)
   }

   server := grpc.NewServer()

   RegisterAdminServer(server, NewAdminService())
   RegisterBizServer(server, NewBizService())
   fmt.Println("starting server at :" + address)
   server.Serve(listener)
}

type Admin struct {
}

func NewAdminService() AdminServer {
   return &Admin{}
}

func (admin Admin) Logging(in *Nothing, logServer Admin_LoggingServer) error {
   log.Default().Println("Logging")
   return nil
}

func (admin Admin) Statistics(stat *StatInterval, statServer Admin_StatisticsServer) error {
   log.Default().Println("Statistics")
   return nil
}

func (admin Admin) mustEmbedUnimplementedAdminServer() {}

type Biz struct {
}

func NewBizService() BizServer {
   return &Biz{}
}

func (biz Biz) Check(ctx context.Context, in *Nothing) (*Nothing, error) {
   log.Default().Println("Check")
   return &Nothing{Dummy: true}, nil
}

func (biz Biz) Add(ctx context.Context, in *Nothing) (*Nothing, error) {
   log.Default().Println("Add")
   return &Nothing{Dummy: true}, nil
}

func (biz Biz) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
   log.Default().Println("Test")
   return &Nothing{Dummy: true}, nil
}

func (biz Biz) mustEmbedUnimplementedBizServer() {
   log.Default().Println("mustEmbedUnimplementedBizServer")
}
