package main

import (
   "context"
   "encoding/json"
   "errors"
   "fmt"
   "log"
   "net"
   "strings"
   "sync"
   "time"

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
   m             *sync.Mutex
   AclData       []ACLDataStruct
   logChan       chan Event
   logListeners  []*logListener
   statListeners []*statListener
}

type logListener struct {
   stream Admin_LoggingServer
}

type statListener struct {
   m                     *sync.RWMutex
   stream                *Admin_StatisticsServer
   statMethodsCounters   map[string]uint64
   statConsumersCounters map[string]uint64
}

func (s *CoreService) sendStatToListeners(method string, consumer string) {
   s.m.Lock()
   for _, listener := range s.statListeners {
      _, ok := listener.statMethodsCounters[method]
      if ok {
         listener.statMethodsCounters[method]++
      } else {
         listener.statMethodsCounters[method] = 1
      }

      _, ok = listener.statConsumersCounters[consumer]
      if ok {
         listener.statConsumersCounters[consumer]++
      } else {
         listener.statConsumersCounters[consumer] = 1
      }
   }
   s.m.Unlock()
}

func (s *CoreService) interceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
   fullMethod := info.FullMethod
   h, _ := handler(ctx, req)
   consumer, err := GetConsumer(ctx)
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

   s.logChan <- newEvent(fullMethod, ctx)

   s.sendStatToListeners(fullMethod, consumer)
   return h, err
}

func (s *CoreService) streamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
   fullMethod := info.FullMethod
   consumer, err := GetConsumer(ss.Context())

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

   _ = handler(srv, ss)
   s.sendStatToListeners(fullMethod, consumer)
   return nil
}

func StartMyMicroservice(ctx context.Context, address string, aclDataJson string) error {
   aclData, err := parseAcl(aclDataJson)
   if err != nil {
      return err
   }
   logsChan := make(chan Event, 10)
   coreService := &CoreService{
      m:       &sync.Mutex{},
      AclData: aclData,
      logChan: logsChan,
   }
   listener, err := net.Listen("tcp", address)
   if err != nil {
      log.Fatalln("can't listen port", err)
      return err
   }

   server := grpc.NewServer(
      grpc.UnaryInterceptor(coreService.interceptor),
      grpc.StreamInterceptor(coreService.streamInterceptor),
   )

   RegisterAdminServer(server, coreService)
   RegisterBizServer(server, coreService)
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
      Timestamp: 0,
      Consumer:  consumer,
      Method:    method,
      Host:      addr,
   }
}

func (s *CoreService) sendLogToAllListeners(e *Event) {
   // s.m.Lock()
   // defer s.m.Unlock()
   for _, l := range s.logListeners {
      err := l.stream.Send(e)
      if err != nil {
         log.Fatal(err)
      }
   }
}

func (s *CoreService) Logging(in *Nothing, stream Admin_LoggingServer) error {
   ctx := stream.Context()
   s.m.Lock()
   lenListeners := len(s.logListeners)
   s.m.Unlock()
   if lenListeners == 0 {
      s.logChan <- newEvent("/main.Admin/Logging", ctx)
   }
   s.addLogListener(&logListener{
      stream: stream,
   })

   for {
      select {
      case logMsg := <-s.logChan:
         s.sendLogToAllListeners(&logMsg)
      case <-ctx.Done():
         log.Printf("Client has disconnected")
         return nil
      }
   }
}

func (s *CoreService) Statistics(stat *StatInterval, stream Admin_StatisticsServer) error {
   l := &statListener{
      m:                     &sync.RWMutex{},
      stream:                &stream,
      statMethodsCounters:   make(map[string]uint64),
      statConsumersCounters: make(map[string]uint64),
   }
   s.m.Lock()
   if len(s.statListeners) == 0 {
      l.statMethodsCounters = map[string]uint64{
         "/main.Admin/Statistics": 1,
      }
      consumer, _ := GetConsumer(stream.Context())
      l.statConsumersCounters = map[string]uint64{
         consumer: 1,
      }
   }
   s.m.Unlock()
   s.addStatListener(l)

   seconds := int64(stat.IntervalSeconds)
   ticker := time.NewTicker(time.Duration(seconds) * time.Second)
   for {
      select {
      case <-ticker.C:
         out := &Stat{
            Timestamp:  0,
            ByMethod:   l.statMethodsCounters,
            ByConsumer: l.statConsumersCounters,
         }
         _ = stream.Send(out)
         s.m.Lock()
         l.statMethodsCounters = make(map[string]uint64)
         l.statConsumersCounters = make(map[string]uint64)
         s.m.Unlock()
      case <-stream.Context().Done():
         ticker.Stop()
         return nil
      }
   }
}

func (s CoreService) mustEmbedUnimplementedAdminServer() {}

func (s CoreService) Check(ctx context.Context, in *Nothing) (*Nothing, error) {
   return in, nil
}

func (s CoreService) Add(ctx context.Context, in *Nothing) (*Nothing, error) {
   return in, nil
}

func (s CoreService) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
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

func (s *CoreService) addStatListener(l *statListener) {
   s.m.Lock()
   defer s.m.Unlock()
   s.statListeners = append(s.statListeners, l)
}

func (s *CoreService) addLogListener(l *logListener) {
   s.m.Lock()
   defer s.m.Unlock()
   s.logListeners = append(s.logListeners, l)
}
