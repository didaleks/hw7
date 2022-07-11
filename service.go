package main

import (
   "context"
   "log"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
type Admin struct {
}

type Biz struct {
}

func (biz *Biz) Check(ctx context.Context, in Nothing) (*Nothing, error) {
   log.Default().Println("Check")
   return &Nothing{Dummy: true}, nil
}

// func (a *Admin) Create(ctx context.Context, in interface{}) (, error) {
//    fmt.Println("call Create", in)
//    id := &session.SessionID{RandStringRunes(sessKeyLen)}
//    sm.mu.Lock()
//    sm.sessions[*id] = in
//    sm.mu.Unlock()
//    return id, nil
// }
