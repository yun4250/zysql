package zysql

import (
	"testing"
	"fmt"
	"time"
)

func Test(t *testing.T) {
	//fn := "spark_35c583bb-dfcc-4799-aba7-a07e7c8557b7.dump"
	//if start, end := strings.Index(fn, "spark_"), strings.Index(fn, ".dump"); start != -1 && end != -1 {
	//	uid := fn[start+len("spark_"):end]
	//	fmt.Println(uid)
	//}

	//f := []string{"1", "2"}
	//cache := make([]string, len(f))
	//copy(cache, f)
	//fmt.Println(f)
	//fmt.Println(cache)

	timer := time.NewTimer(time.Second * 1)
	go func() {
		for {
			<-timer.C
			fmt.Println("executed")
		}
	}()
	if !timer.Stop() {
		<-timer.C
	}
	time.Sleep(time.Second * 3)
	fmt.Printf("reset: %t\n",timer.Reset(time.Second * 2))
	time.Sleep(time.Second * 3)
	fmt.Printf("reset: %t\n",timer.Reset(time.Second * 2))
	if !timer.Stop() {
		<-timer.C
		fmt.Println("executed")
	}
	time.Sleep(time.Second * 3)
	}
