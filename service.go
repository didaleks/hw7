package main

import (
   "context"
   "encoding/json"
   "log"
   "net"

   "google.golang.org/grpc"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
// func main() {
//    // запуск сервиса
//    address := "127.0.0.1:8082"
//    ctx := context.Background()
//    StartMyMicroservice(ctx, address, "")
//    fmt.Scanln()
// }

func StartMyMicroservice(ctx context.Context, address string, aclDataJson string) error {
   var aclDataMap map[string]interface{}
   err := json.Unmarshal([]byte(aclDataJson), &aclDataMap)
   if err != nil {
      return err
   }

   listener, err := net.Listen("tcp", address)
   if err != nil {
      log.Fatalln("can't listen port", err)
      return err
   }

   server := grpc.NewServer()
   RegisterAdminServer(server, NewAdminService())
   RegisterBizServer(server, NewBizService())
   go func() {
      server.Serve(listener)
   }()
   go func(ctx context.Context, server *grpc.Server) {
      select {
      case <-ctx.Done():
         server.Stop()
      }
   }(ctx, server)

   return err
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
