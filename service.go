package main

import (
   "context"
   "encoding/json"
   "errors"
   "fmt"
   "log"
   "net"
   "strings"

   "google.golang.org/grpc"
   "google.golang.org/grpc/codes"
   "google.golang.org/grpc/metadata"
   "google.golang.org/grpc/status"
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

type ACLDataStruct struct {
   Consumer string
   Methods  []string
}

type Middleware struct {
   AclData []ACLDataStruct
}

func (m Middleware) aclInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
   fullMethod := info.FullMethod
   h, _ := handler(ctx, req)
   consumer, err := GetConsumer(ctx)
   fmt.Println("aclInterceptor method", fullMethod)
   fmt.Println(fullMethod, consumer)
   if err != nil {
      return h, status.Error(codes.Unauthenticated, err.Error())
   }

   allowed, err := IsAllowedMethod(consumer, fullMethod, m.AclData)
   if err != nil {
      return h, status.Error(codes.Unauthenticated, err.Error())
   }
   if !allowed {
      return h, status.Error(codes.Unauthenticated, "disallowed method")
   }

   return h, err
}

func StartMyMicroservice(ctx context.Context, address string, aclDataJson string) error {
   var aclDataMap map[string][]string
   err := json.Unmarshal([]byte(aclDataJson), &aclDataMap)
   if err != nil {
      return err
   }

   var aclData []ACLDataStruct
   for consumer, aclDatum := range aclDataMap {
      for i, method := range aclDatum {
         aclDatum[i] = method
      }
      aclData = append(aclData, ACLDataStruct{Consumer: consumer, Methods: aclDatum})
   }

   middleware := Middleware{AclData: aclData}
   listener, err := net.Listen("tcp", address)
   if err != nil {
      log.Fatalln("can't listen port", err)
      return err
   }

   server := grpc.NewServer(
      grpc.UnaryInterceptor(middleware.aclInterceptor),
   )
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
   ACLData []ACLDataStruct
}

func IsAllowedMethod(consumer string, inMethod string, rules []ACLDataStruct) (bool, error) {
   consumerExists := false
   for _, rule := range rules {
      if rule.Consumer == consumer {
         consumerExists = true
         for _, ruleMethod := range rule.Methods {
            if ruleMethod == inMethod {
               return true, nil
            }

            if strings.Contains(ruleMethod, "*") {
               ruleMethodParts := strings.Split(ruleMethod, ".")
               ruleMethodParts = ruleMethodParts[:len(ruleMethodParts)-1]
               ruleMethodPackage := strings.Join(ruleMethodParts, ".")

               inMethodParts := strings.Split(inMethod, ".")
               inMethodParts = inMethodParts[:len(inMethodParts)-1]
               inMethodPackage := strings.Join(inMethodParts, ".")

               if ruleMethodPackage == inMethodPackage {
                  return true, nil
               }
            }
         }
      }
   }

   if !consumerExists {
      return false, errors.New("consumer not exists")
   }

   return false, nil
}

func NewAdminService() AdminServer {
   return &Admin{}
}

func (admin Admin) Logging(in *Nothing, logServer Admin_LoggingServer) error {
   return nil
}

func (admin Admin) Statistics(stat *StatInterval, statServer Admin_StatisticsServer) error {
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
   return in, nil
}

func (biz Biz) Add(ctx context.Context, in *Nothing) (*Nothing, error) {
   log.Default().Println("Add")
   return in, nil
}

func (biz Biz) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
   return in, nil
}

func (biz Biz) mustEmbedUnimplementedBizServer() {
   log.Default().Println("mustEmbedUnimplementedBizServer")
}

func GetConsumer(ctx context.Context) (string, error) {
   md, _ := metadata.FromIncomingContext(ctx)
   if consumer, found := md["consumer"]; found {
      return consumer[0], nil
   }

   return "", errors.New("no consumer data")
}
