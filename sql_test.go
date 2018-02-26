package zysql

import (
	"testing"
	_ "github.com/kshvakov/clickhouse"
)

func Test(t *testing.T) {
	//if conn, e := GetConn("clickhouse", "tcp://10.13.224.150:9000?debug=true&database=log"); e == nil {
	//	fmt.Println("connected")
	//	time.Sleep(time.Second * 20)
	//	fmt.Println(conn.PingContext(context.Background()))
	//	conn.Close()
	//}else{
	//	fmt.Println("disconnected")
	//	fmt.Println(e)
	//}

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

	//timer := time.NewTimer(time.Second * 1)
	//go func() {
	//	for {
	//		<-timer.C
	//		fmt.Println("executed")
	//	}
	//}()
	//if !timer.Stop() {
	//	<-timer.C
	//}
	//time.Sleep(time.Second * 3)
	//fmt.Printf("reset: %t\n", timer.Reset(time.Second*2))
	//time.Sleep(time.Second * 3)
	//fmt.Printf("reset: %t\n", timer.Reset(time.Second*2))
	//if !timer.Stop() {
	//	<-timer.C
	//	fmt.Println("executed")
	//}
	//time.Sleep(time.Second * 3)
}
