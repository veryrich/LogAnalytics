package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb1-client/v2"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// reader 接口 定义了Read方法的规范
type Reader interface {
	Read(rc chan []byte)
}

// write 接口 定义了write方法的规范
type Write interface {
	Write(wc chan *Message)
}

// 定义一个结构体，包含两个通道和两个方法，相当于面向对象中的类
type LogProcess struct {
	rc    chan []byte
	wc    chan *Message
	read  Reader
	write Write
}

// 用于存储日志文件路径的结构体，path属性相当于面向对象中的类属性
type ReadFromFile struct {
	path string // 读取文件的路径
}

// 用于存储influxDB数据库源的结构体，influxDBDsn 相当于面向对象中的类属性
type WriteToInfluxDB struct {
	influxDBDsn string // influxDB data source
}

// 用于存储日志的结构体，里面的属相相当于面向对象中的类属性
type Message struct {
	TimeLocal                    time.Time
	ByteSend                     int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}

type SystemInfo struct {
	HandleLine   int     `json:"HandleLine"`   // 总处理日志行数
	Tps          float64 `json:"tps"`          // 系统吞吐量
	ReadChanLen  int     `json:"readChanLen"`  // read channel 长度
	WriteChanLen int     `json:"writeChanLen"` // write channel 长度
	RunTime      string  `json:"runtime"`      // 运行总时间
	ErrNum       int     `json:"errNum"`       // 错误数
}

const (
	TypeHandleLine = 0
	TypeErrNum     = 1
)

var TypeMonitorChan = make(chan int, 20)

type Monitor struct {
	startTime time.Time
	data      SystemInfo
	tpsSli    []int
}

func (m *Monitor) start(lp *LogProcess) {

	// 开始监控

	// 统计数据,错误数与共统计了多少行日志
	go func() {
		for n := range TypeMonitorChan {
			switch n {
			case TypeErrNum:
				m.data.ErrNum += 1
			case TypeHandleLine:
				m.data.HandleLine += 1
			}
		}
	}()

	// 定时器 每五秒计算一次系统总吞吐量
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			<-ticker.C
			m.tpsSli = append(m.tpsSli, m.data.HandleLine)
			if len(m.tpsSli) > 2 {
				m.tpsSli = m.tpsSli[1:]
			}
		}
	}()

	// 创建一个HttpHandle，来监听端口，并提供监控数据
	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		// 对m中的结构体进行赋值，相当于面向对象中，继承来的类属性
		m.data.RunTime = time.Now().Sub(m.startTime).String()
		m.data.ReadChanLen = len(lp.rc)
		m.data.WriteChanLen = len(lp.wc)

		if len(m.tpsSli) >= 2 {
			m.data.Tps = float64(m.tpsSli[1] - m.tpsSli[0]/5)
		}

		ret, _ := json.MarshalIndent(m.data, "", "\t")

		io.WriteString(writer, string(ret))
	})

	http.ListenAndServe(":9193", nil)
}

// Read方法，用于读取日志，绑定到了ReadFromFile结构体中，相当于面向对象中的类方法
// 若要使用，实例化ReadFromFile结构体后，r = ReadFromFile{init something}, r.Read(arg chan []byte)
func (r *ReadFromFile) Read(rc chan []byte) {
	// 读取模块

	// 1. 打开文件
	f, err := os.Open(r.path)
	if err != nil {
		panic(fmt.Sprintf("open file error:%s", err.Error()))
	}

	// 2. 从文件末尾开始逐行读取文件内容
	f.Seek(0, 2)

	reader := bufio.NewReader(f)

	for {

		line, err := reader.ReadBytes('\n')

		// 如果是文件结束,代表暂无日志,或者日志读取速度比日志写入速度快,500ms后重试
		if err == io.EOF {
			time.Sleep(100 * time.Millisecond)
			continue
		} else if err != nil {
			panic(fmt.Sprintf("ReadBytes error:%s", err.Error()))
		}

		TypeMonitorChan <- TypeHandleLine // 存入条目，用作监控取数据
		rc <- line[:len(line)-1]
	}

}

// Write方法，用于写入日志到数据库，绑定到了WriteToInfluxDB结构体中，相当于面向对象中的类方法
// 若要使用，实例化WriteToInfluxDB结构体后，w = WriteToInfluxDB{init something}, w.Write(arg chan *Message)
func (w *WriteToInfluxDB) Write(wc chan *Message) {
	// 写入模块

	infSli := strings.Split(w.influxDBDsn, "@")

	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     infSli[0],
		Username: infSli[1],
		Password: infSli[2],
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  infSli[3],
		Precision: infSli[4],
	})
	if err != nil {
		log.Fatal(err)
	}

	for v := range wc {
		// Create a point and add to batch
		// Tags: Path, Method, Scheme, Status

		tags := map[string]string{"Path": v.Path, "Method": v.Method, "Scheme": v.Scheme, "Status": v.Status}

		// Fields: UpstreamTime, RequestTime, BytesSend
		fields := map[string]interface{}{
			"UpstreamTime": v.UpstreamTime,
			"RequestTime":  v.RequestTime,
			"BytesSend":    v.ByteSend,
		}

		pt, err := client.NewPoint("nginx_log", tags, fields, v.TimeLocal)
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)

		// Write the batch
		if err := c.Write(bp); err != nil {
			log.Fatal(err)
		}

		log.Println("write success")

		// Close client resources
		if err := c.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

// Process方法，解析每一行的日志，并拆分需要提取的内容，绑定到了LogProcess结构体中，相当于面向对象中的类方法
// 若要使用，实例化LogProcess结构体后，l = *LogProcess{init something}, l.Process()
func (l *LogProcess) Process() {
	// 解析模块

	// 正则表达式，过滤并拆分每行日志
	r := regexp.MustCompile(`([\d\,]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)

	// 设置时区
	location, _ := time.LoadLocation("Asia/Shanghai")

	// 从channel中按行读取数据
	for v := range l.rc {
		ret := r.FindStringSubmatch(string(v))

		// 解析完,每行日志应该会拆分成14个元素,所以判断,多了少了都不正确
		if len(ret) != 14 {
			TypeMonitorChan <- TypeErrNum
			log.Println("FindStringSubmatch fail:", string(v))
			continue
		}

		message := &Message{} // 实例化 Message , 相当于面向对象中的实例化

		// 设置一个模板时间
		inLocation, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], location)
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Println("ParseInLocation fail:", err.Error(), ret[4])
			continue
		}

		message.TimeLocal = inLocation // 结构体赋值

		byteSent, _ := strconv.Atoi(ret[8]) // 字符串转数字
		message.ByteSend = byteSent

		// 按空格拆分字符串,按照预定义的日志格式,此处拆分好后,会有3个元素,多了少了都不正确
		reqSli := strings.Split(ret[6], " ")
		if len(reqSli) != 3 {
			TypeMonitorChan <- TypeErrNum
			log.Println("strings Split fail:", ret[6])
			continue
		}

		message.Method = reqSli[0]

		// 将字符串的url,转成URL
		parse, err := url.Parse(reqSli[1])
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Println("url Parse fail", err.Error())
			continue
		}

		message.Path = parse.Path

		message.Scheme = ret[5]

		message.Status = ret[7]

		// 将字符串的小数,转换成float类型的小数,第二个参数是小数的精度,此处为float64
		upstreamTime, _ := strconv.ParseFloat(ret[12], 64)
		requestTIme, _ := strconv.ParseFloat(ret[13], 64)

		message.UpstreamTime = upstreamTime
		message.RequestTime = requestTIme

		l.wc <- message
	}
}

func main() {
	// 主函数入口

	// 设置配置信息为参数，在执行时 ./lao_process -path VALUE  -fluxDsn VALUE
	var path, influxDsn string
	flag.StringVar(&path, "path", "./access.log", "read file path")
	flag.StringVar(&influxDsn, "influxDsn", "http://127.0.0.1:8086@log@log@logs@s", "influx data source")
	flag.Parse()

	r := &ReadFromFile{
		path: path,
	}

	w := &WriteToInfluxDB{
		influxDBDsn: influxDsn,
	}

	//因为两个函数都使用到了LogProcess结构体，所以提取出来，以参数形式传入两个需要用到此结构体的goroutine
	lp := &LogProcess{
		rc:    make(chan []byte, 200), // 200 给一个缓存，读取肯定比解析慢
		wc:    make(chan *Message, 200),
		read:  r, // 此处传入ReadFromFile结构体，结构体中不止有值，还绑定了Read方法，相当于面向对象中的类方法和类属性，可以直接通过lp.read.Read使用(lp.类属性.类方法)
		write: w, // 接上行：相当于 lp.传入的结构体.传入的结构体中绑定的方法,类似类的继承和多态
	}

	go lp.read.Read(lp.rc) // 读取速度快，一个goroutine足够

	// 处理速度相比读取速度慢，所以开两个goroutine
	for i := 0; i < 2; i++ {
		go lp.Process()
	}

	// 写入速度最慢，因为需要连接远程数据库，有网络io等问题，所以开4个goroutine
	for i := 0; i < 4; i++ {
		go lp.write.Write(lp.wc)
	}

	m := &Monitor{
		startTime: time.Now(),
		data:      SystemInfo{},
	}

	m.start(lp)

}
