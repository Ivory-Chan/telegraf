package modbusx

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	mb "github.com/grid-x/modbus"
	"github.com/influxdata/telegraf"
)

type ModbusWriteCommand struct {
	RedisUrl     string `toml:"redis_url"`
	RedisPass    string `toml:"redis_pass"`
	RedisChannel string `toml:"redis_channel"`
}

type writeCommand struct {
	CollectControlNodeId int     `json:"collectControlNodeId"`
	Controller           string  `json:"controller"`
	SlaveId              int     `json:"slaveId"`
	BaudRate             int     `json:"baudRate"`
	DataBits             int     `json:"dataBits"`
	Parity               string  `json:"parity"`
	StopBits             int     `json:"stopBits"`
	TransmissionMode     string  `json:"transmissionMode"`
	ConfigCode           string  `json:"configCode"`
	MetricCode           string  `json:"metricCode"`
	FuncCode             int     `json:"funcCode"`
	Address              int     `json:"address"`
	Scale                float64 `json:"scale"`
	Value                float64 `json:"value"`
}

type ModbusWrite struct {
	Log telegraf.Logger `toml:"-"`
	ModbusWriteCommand
}

var WriteClient *redis.PubSubConn
var IsRun = 0
var RedisConn redis.Conn

// var err error
var tryNum = 0

func init() {
	var modbusRedis ModbusWrite
	tryTimes.Map = make(map[string]int)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("写服务启动失败一分钟后重试。")
				time.Sleep(60 * time.Second)
				err := modbusRedis.ModbusWriteStart("127.0.0.1:6379", "Dtct@123", "modbusx_write_command")
				if err != nil {
					fmt.Printf("写服务启动失败。")
				}
			}
		}()
		time.Sleep(3 * time.Second)
		err := modbusRedis.ModbusWriteStart("127.0.0.1:6379", "Dtct@123", "modbusx_write_command")
		if err != nil {
			fmt.Printf("写服务启动失败。")
		}
	}()
}

func (m *ModbusWrite) ModbusWriteStart(RedisUrl, RedisPass, RedisChannel string) error {
	var err error
	if IsRun == 1 {
		return errors.New("写服务已运行！")
	}

	fmt.Printf("写服务已启动！\n")

	RedisConn, err = redis.Dial("tcp", RedisUrl)

	if err != nil {
		fmt.Printf("redis dial failed: %v", err)
		return nil
	}
	_, err = RedisConn.Do("auth", RedisPass)
	if err != nil {
		fmt.Printf("redis auth failed: %v RedisUrl:%s RedisPass:%s RedisChannel:%s\n", err,
			m.RedisUrl, m.RedisPass, m.RedisChannel)
		err := RedisConn.Close()
		if err != nil {
			return err
		}
		return nil
	}

	WriteClient = &redis.PubSubConn{Conn: RedisConn}
	err = WriteClient.Subscribe(RedisChannel)

	if err != nil {
		fmt.Printf("redis Subscribe error: %v", err)
		return nil
	}
	IsRun = 1
	m.RedisClientRun(RedisUrl, RedisPass, RedisChannel)
	return nil
}

func (m *ModbusWrite) RedisClientRun(RedisUrl, RedisPass, RedisChannel string) {
	var err error
	go func() {
		for {
			if WriteClient == nil {
				break
			}
			fmt.Printf("wait...")
			switch res := WriteClient.Receive().(type) {
			case redis.Message:
				channel := res.Channel
				message := string(res.Data)
				fmt.Printf("channel: %v, message: %v", channel, message)
				var cmd writeCommand
				var step1 string
				err = json.Unmarshal([]byte(message), &step1)
				if err != nil {
					err = json.Unmarshal([]byte(message), &cmd)
					if err != nil {
						fmt.Printf("命令错误: %s %s \n", message, err)
						continue
					}
				} else {
					err = json.Unmarshal([]byte(step1), &cmd)
					if err != nil {
						fmt.Printf("命令错误: %s %s \n", message, err)
						continue
					}
				}
				fmt.Printf("Controller: %s %.2f \n", cmd.Controller, cmd.Value)
				go func() {
					err = m.Writetask(cmd)
					if err != nil {
						fmt.Printf("Writetask ERROR %v \n", err)
					}
				}()

			case redis.Subscription:
				fmt.Printf("%s: %s %d", res.Channel, res.Kind, res.Count)
			case error:
				// TODO REDIS 掉线处理
				fmt.Printf("error handle...: %v", res.Error())
				//WriteClient.Close()
				//RedisConn.Close()
				IsRun = 0
				tryNum++
				err = m.ModbusWriteStart(RedisUrl, RedisPass, RedisChannel)
				if err != nil {
					fmt.Printf("Redis 连接错误准备重连 %d 次 ERR: %v \n", tryNum, err)
				}
				time.Sleep(30 * time.Second)
				continue
			}
		}
	}()

}

func (m *ModbusWrite) Writetask(cmd writeCommand) error {
	var tmpHandler mb.ClientHandler
	var tmpLock *sync.Mutex

	var timeoutTmp time.Duration
	timeoutTmp = 500 * time.Millisecond

	u, err := url.Parse(cmd.Controller)
	if err != nil {
		return err
	}
	switch u.Scheme {
	case "tcp":
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			return err
		}
		var handler_tmp mb.ClientHandler
		switch cmd.TransmissionMode {
		case "RTUoverTCP":

			var lock *sync.Mutex
			for _, RTUOverTCPClientHandler := range RTUOverTCPClientHandlerCache {
				if RTUOverTCPClientHandler.Controller == cmd.Controller {
					handler_tmp = RTUOverTCPClientHandler.handler
					lock = &RTUOverTCPClientHandler.CacheLock
					break
				}
			}

			if handler_tmp == nil {
				handler := mb.NewRTUOverTCPClientHandler(host + ":" + port)
				handler.Timeout = timeoutTmp
				tmpHandler = handler

				// 存入緩存
				var RTUOverTCPClientHandlerTmp RTUOverTCPClientHandlerMap
				RTUOverTCPClientHandlerTmp.handler = handler
				RTUOverTCPClientHandlerTmp.Controller = cmd.Controller
				tmpLock = &RTUOverTCPClientHandlerTmp.CacheLock
				RTUOverTCPClientHandlerCache = append(RTUOverTCPClientHandlerCache, &RTUOverTCPClientHandlerTmp)
				fmt.Printf("RTUOverTCP 创建串口实例 %s \n", u.Path)
			} else {
				tmpHandler = handler_tmp
				tmpLock = lock
				fmt.Printf("RTUOverTCP 使用缓存句柄 %s \n", u.Path)
			}

		case "ASCIIoverTCP":

			var lock *sync.Mutex
			for _, ASCIIOverTCPClientHandler := range ASCIIOverTCPClientHandlerCache {
				if ASCIIOverTCPClientHandler.Controller == cmd.Controller {
					handler_tmp = ASCIIOverTCPClientHandler.handler
					lock = &ASCIIOverTCPClientHandler.CacheLock
					break
				}
			}

			if handler_tmp == nil {
				handler := mb.NewASCIIOverTCPClientHandler(host + ":" + port)
				handler.Timeout = timeoutTmp
				tmpHandler = handler

				// 存入緩存
				var ASCIIOverTCPClientHandlerTmp ASCIIOverTCPClientHandlerMap
				ASCIIOverTCPClientHandlerTmp.handler = handler
				ASCIIOverTCPClientHandlerTmp.Controller = cmd.Controller
				tmpLock = &ASCIIOverTCPClientHandlerTmp.CacheLock
				ASCIIOverTCPClientHandlerCache = append(ASCIIOverTCPClientHandlerCache, &ASCIIOverTCPClientHandlerTmp)
				fmt.Printf("ASCIIoverTCP 创建串口实例 %s \n", u.Path)
			} else {
				tmpHandler = handler_tmp
				tmpLock = lock
				fmt.Printf("ASCIIoverTCP 使用缓存句柄 %s \n", u.Path)
			}

		default:
			handler := mb.NewTCPClientHandler(host + ":" + port)
			handler.Timeout = timeoutTmp
			tmpHandler = handler
		}
	case "file":
		var handlerT mb.ClientHandler
		switch cmd.TransmissionMode {
		case "RTU":
			var lockT *sync.Mutex
			for _, RTUClientHandler := range RTUClientHandlerCache {
				if RTUClientHandler.Controller == u.Path {
					handlerT = RTUClientHandler.handler
					lockT = &RTUClientHandler.CacheLock
					break
				}
			}
			if handlerT == nil {
				handler := mb.NewRTUClientHandler(u.Path)
				handler.Timeout = time.Duration(1 * time.Second)
				handler.BaudRate = cmd.BaudRate
				handler.DataBits = cmd.DataBits
				handler.Parity = cmd.Parity
				handler.StopBits = cmd.StopBits
				tmpHandler = handler

				// 存入緩存
				var RTUClientHandlerTmp RTUClientHandlerMap
				RTUClientHandlerTmp.handler = handler
				RTUClientHandlerTmp.Controller = u.Path
				tmpLock = &RTUClientHandlerTmp.CacheLock
				RTUClientHandlerCache = append(RTUClientHandlerCache, &RTUClientHandlerTmp)
				fmt.Printf("Writetask RTU 创建串口实例 %s \n", u.Path)
			} else {
				tmpHandler = handlerT
				tmpLock = lockT
				fmt.Printf("Writetask RTU 使用缓存句柄 %s \n", u.Path)
			}
		case "ASCII":
			var lock *sync.Mutex
			for _, ASCIIClientHandler := range ASCIIClientHandlerCache {
				if ASCIIClientHandler.Controller == u.Path {
					lock = &ASCIIClientHandler.CacheLock
					handlerT = ASCIIClientHandler.handler
					break
				}
			}
			if handlerT == nil {
				handler := mb.NewASCIIClientHandler(u.Path)
				handler.Timeout = time.Duration(1 * time.Second)
				handler.BaudRate = cmd.BaudRate
				handler.DataBits = cmd.DataBits
				handler.Parity = cmd.Parity
				handler.StopBits = cmd.StopBits
				tmpHandler = handler
				// 存入緩存
				var ASCIIClientHandlerTmp ASCIIClientHandlerMap
				ASCIIClientHandlerTmp.handler = handler
				ASCIIClientHandlerTmp.Controller = u.Path
				tmpLock = &ASCIIClientHandlerTmp.CacheLock
				ASCIIClientHandlerCache = append(ASCIIClientHandlerCache, &ASCIIClientHandlerTmp)
				fmt.Printf("ASCII 创建串口实例 %s \n", u.Path)
			} else {
				tmpHandler = handlerT
				tmpLock = lock
				fmt.Printf("ASCII 使用缓存句柄 %s \n", u.Path)
			}
		default:
			return fmt.Errorf("invalid protocol '%s' - '%s' ", u.Scheme, cmd.TransmissionMode)
		}

	default:
		return fmt.Errorf("invalid controller %q", cmd.Controller)
	}

	if u.Scheme == "file" {
		if tmpLock != nil {
			fmt.Printf("获得锁 %s \n", cmd.Controller)
			tmpLock.Lock()
			defer func() {
				time.Sleep(50 * time.Millisecond)
				fmt.Printf("释放锁 %s \n", cmd.Controller)
				tmpLock.Unlock()
			}()
		}
	}

	tmpHandler.SetSlave(byte(cmd.SlaveId))
	client := mb.NewClient(tmpHandler)

	if cmd.FuncCode == 5 {
		var value uint16
		if cmd.Value == 0 {
			value = 0x0000
		} else {
			value = 0xFF00
		}
		_, err = client.WriteSingleCoil(uint16(cmd.Address), value)
		if err != nil {
			fmt.Printf("write WriteSingleCoil error: %v\n", err)
		} else {
			fmt.Printf("写入成功 %s %d %.2f\n", cmd.Controller, cmd.Address, cmd.Value)

		}
	} else if cmd.FuncCode == 16 {
		d := make([]byte, 2)
		binary.BigEndian.PutUint16(d[0:], uint16(cmd.Value))
		_, err = client.WriteMultipleRegisters(uint16(cmd.Address), 1, d)
		if err != nil {
			fmt.Printf("write WriteMultipleRegisters error: %v\n", err)
		} else {
			fmt.Printf("写入成功 %s %d %.2f\n", cmd.Controller, cmd.Address, cmd.Value)
		}
	} else {
		_, err = client.WriteSingleRegister(uint16(cmd.Address), uint16(cmd.Value))
		if err != nil {
			fmt.Printf("write WriteSingleRegister error: %v\n", err)
		} else {
			fmt.Printf("写入成功 %s %d %.2f\n", cmd.Controller, cmd.Address, cmd.Value)
		}
	}
	return nil
}
