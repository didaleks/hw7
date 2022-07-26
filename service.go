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
   "google.golang.org/grpc/peer"
   "google.golang.org/grpc/status"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
// func main() {
//    // запуск сервиса
//    address := "127.0.0.1:8082"
//    ctx := context.Background()
//    err := StartMyMicroservice(ctx, address, ACLData)
//    if err != nil {
//       log.Fatal(err)
//       return
//    }
//    fmt.Scanln()
// }

type ACLDataStruct struct {
   Consumer string
   Methods  []string
}

type CoreService struct {
   AclData []ACLDataStruct
   logChan chan Event
}

func (s CoreService) interceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
   fullMethod := info.FullMethod
   h, _ := handler(ctx, req)
   consumer, err := GetConsumer(ctx)
   fmt.Println("interceptor method", fullMethod)
   fmt.Println(fullMethod, consumer)
   if err != nil {
      return h, status.Error(codes.Unauthenticated, err.Error())
   }

   allowed, err := IsAllowedMethod(consumer, fullMethod, s.AclData)
   if err != nil {
      return h, status.Error(codes.Unauthenticated, err.Error())
   }
   if !allowed {
      return h, status.Error(codes.Unauthenticated, "disallowed method")
   }

   return h, err
}

func (s CoreService) streamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
   fullMethod := info.FullMethod
   consumer, err := GetConsumer(ss.Context())
   handler(srv, ss)
   fmt.Println(fullMethod, consumer)
   if err != nil {
      return status.Error(codes.Unauthenticated, err.Error())
   }
   allowed, err := IsAllowedMethod(consumer, fullMethod, s.AclData)
   if err != nil {
      return status.Error(codes.Unauthenticated, err.Error())
   }
   if !allowed {
      return status.Error(codes.Unauthenticated, "disallowed method")
   }
   return nil
}

func StartMyMicroservice(ctx context.Context, address string, aclDataJson string) error {
   aclData, err := parseAcl(aclDataJson)
   if err != nil {
      return err
   }
   middleware := CoreService{AclData: aclData}
   listener, err := net.Listen("tcp", address)
   if err != nil {
      log.Fatalln("can't listen port", err)
      return err
   }

   server := grpc.NewServer(
      grpc.UnaryInterceptor(middleware.interceptor),
      grpc.StreamInterceptor(middleware.streamInterceptor),
   )
   logsChan := make(chan Event, 10)

   service := CoreService{logChan: logsChan}
   RegisterAdminServer(server, service)
   RegisterBizServer(server, service)
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

func newEvent(method string, ctx context.Context) Event {
   p, _ := peer.FromContext(ctx)
   addr := p.Addr.String()
   consumer, _ := GetConsumer(ctx)
   return Event{
      Consumer: consumer,
      Method:   method,
      Host:     addr,
   }
}

func (s CoreService) Logging(in *Nothing, stream Admin_LoggingServer) error {
   go func() {
      for logMsg := range s.logChan {
         fmt.Println("logMsg", logMsg)
         err := stream.Send(&logMsg)
         if err != nil {
            log.Fatal(err)
            return
         }
      }
   }()
   ctx := stream.Context()
   event := newEvent("/main.Admin/Logging", ctx)
   s.logChan <- event

   for {
      select {
      case <-ctx.Done():
         log.Printf("Client has disconnected")
         return nil
      }
   }
}

func (s CoreService) Statistics(stat *StatInterval, stream Admin_StatisticsServer) error {
   // consumer, _ := GetConsumer(stream.Context())
   log.Default().Println("Statistics method")
   out := &Stat{}
   stream.Send(out)
   return nil
}

func (s CoreService) mustEmbedUnimplementedAdminServer() {}

func (s CoreService) Check(ctx context.Context, in *Nothing) (*Nothing, error) {
   event := newEvent("/main.Biz/Check", ctx)
   s.logChan <- event
   return in, nil
}

func (s CoreService) Add(ctx context.Context, in *Nothing) (*Nothing, error) {
   event := newEvent("/main.Biz/Add", ctx)
   s.logChan <- event
   return in, nil
}

func (s CoreService) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
   event := newEvent("/main.Biz/Test", ctx)
   s.logChan <- event
   return in, nil
}

func (s CoreService) mustEmbedUnimplementedBizServer() {
   log.Default().Println("mustEmbedUnimplementedBizServer")
}

func GetConsumer(ctx context.Context) (string, error) {
   md, _ := metadata.FromIncomingContext(ctx)
   if consumer, found := md["consumer"]; found {
      return consumer[0], nil
   }

   return "", errors.New("no consumer data")
}

func parseAcl(aclDataJson string) ([]ACLDataStruct, error) {
   var aclDataMap map[string][]string
   err := json.Unmarshal([]byte(aclDataJson), &aclDataMap)
   if err != nil {
      return nil, err
   }

   var aclData []ACLDataStruct
   for consumer, aclDatum := range aclDataMap {
      for i, method := range aclDatum {
         aclDatum[i] = method
      }
      aclData = append(aclData, ACLDataStruct{Consumer: consumer, Methods: aclDatum})
   }

   return aclData, nil
}
