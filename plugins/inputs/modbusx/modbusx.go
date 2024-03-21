//go:build !openbsd

package modbusx

import (
	"errors"
	"fmt"
	mb "github.com/grid-x/modbus"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/inputs"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ModbusWorkarounds struct {
	PollPause        config.Duration `toml:"pause_between_requests"`
	CloseAfterGather bool            `toml:"close_connection_after_gather"`
}

// Modbus holds all data relevant to the plugin
type ModbusX struct {
	Name             string             `toml:"name"`
	Controller       string             `toml:"controller"`
	TransmissionMode string             `toml:"transmission_mode"`
	BaudRate         int                `toml:"baud_rate"`
	DataBits         int                `toml:"data_bits"`
	Parity           string             `toml:"parity"`
	StopBits         int                `toml:"stop_bits"`
	Timeout          config.Duration    `toml:"timeout"`
	Retries          int                `toml:"busy_retries"`
	RetriesWaitTime  config.Duration    `toml:"busy_retries_wait"`
	DebugConnection  bool               `toml:"debug_connection"`
	Workarounds      ModbusWorkarounds  `toml:"workarounds"`
	WriteCommand     ModbusWriteCommand `toml:"write_command"`
	Log              telegraf.Logger    `toml:"-"`
	// Register configuration
	ConfigurationOriginal
	// Connection handling
	client      mb.Client
	handler     mb.ClientHandler
	isConnected bool
	// Request handling
	requests  map[byte]requestSet
	CacheLock *sync.Mutex
	ReqTimes  int

	BlockReqAllTryTimes int
	BlockReqNowTryTimes int
	BlockIntervalTime   int

	NodeId int

	ReqStatus []reqStatus
	ReqLock   sync.Mutex
}
type SafeMap struct {
	mu  sync.Mutex
	Map map[string]int
}

func (sm *SafeMap) Get(key string) int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	value, ok := sm.Map[key]
	if ok {
		return value
	}
	sm.Map[key] = 0
	time.Sleep(5 * time.Millisecond)
	return 0
}

func (sm *SafeMap) Set(key string, value int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Map[key] = value
	time.Sleep(5 * time.Millisecond)
}

var tryTimes SafeMap

type RTUClientHandlerMap struct {
	Controller string
	handler    *mb.RTUClientHandler
	CacheLock  sync.Mutex
}

type ASCIIClientHandlerMap struct {
	Controller string
	handler    *mb.ASCIIClientHandler
	CacheLock  sync.Mutex
}

type RTUOverTCPClientHandlerMap struct {
	Controller string
	handler    *mb.RTUOverTCPClientHandler
	CacheLock  sync.Mutex
}

type ASCIIOverTCPClientHandlerMap struct {
	Controller string
	handler    *mb.ASCIIOverTCPClientHandler
	CacheLock  sync.Mutex
}

var RTUOverTCPClientHandlerCache []*RTUOverTCPClientHandlerMap
var ASCIIOverTCPClientHandlerCache []*ASCIIOverTCPClientHandlerMap

var RTUClientHandlerCache []*RTUClientHandlerMap
var ASCIIClientHandlerCache []*ASCIIClientHandlerMap

type fieldConverterFunc func(bytes []byte) interface{}

type requestSet struct {
	coil     []request
	discrete []request
	holding  []request
	input    []request
}

type field struct {
	measurement string
	name        string
	scale       float64
	address     uint16
	length      uint16
	converter   fieldConverterFunc
	value       interface{}
}

const (
	cDiscreteInputs   = "discrete_input"
	cCoils            = "coil"
	cHoldingRegisters = "holding_register"
	cInputRegisters   = "input_register"
)

const description = `Retrieve data from MODBUS slave devices`
const sampleConfig = `
  ## Connection Configuration
  ##
  ## The plugin supports connections to PLCs via MODBUS/TCP, RTU over TCP, ASCII over TCP or
  ## via serial line communication in binary (RTU) or readable (ASCII) encoding
  ##
  ## Device name
  name = "Device"

  ## dynamic_interval : if true, new job will start after the end of last job immediately！！！
  dynamic_interval = false

  ## Slave ID - addresses a MODBUS device on the bus
  ## Range: 0 - 255 [0 = broadcast; 248 - 255 = reserved]
  slave_id = 1

  ## Timeout for each request
  timeout = "1s"

  ## Maximum number of retries and the time to wait between retries
  ## when a slave-device is busy.
  # busy_retries = 0
  # busy_retries_wait = "100ms"

  # TCP - connect via Modbus/TCP
  controller = "tcp://localhost:502"

  ## Serial (RS485; RS232)
  # controller = "file:///dev/ttyUSB0"
  # baud_rate = 9600
  # data_bits = 8
  # parity = "N"
  # stop_bits = 1
  # transmission_mode = "RTU"

  ## Trace the connection to the modbus device as debug messages
  ## Note: You have to enable telegraf's debug mode to see those messages!
  # debug_connection = false

  ## For Modbus over TCP you can choose between "TCP", "RTUoverTCP" and "ASCIIoverTCP"
  ## default behaviour is "TCP" if the controller is TCP
  ## For Serial you can choose between "RTU" and "ASCII"
  # transmission_mode = "RTU"

  ## Measurements
  ##

  ## Digital Variables, Discrete Inputs and Coils
  ## measurement - the (optional) measurement name, defaults to "modbus"
  ## name        - the variable name
  ## address     - variable address

  discrete_inputs = [
    { name = "start",          address = [0]},
    { name = "stop",           address = [1]},
    { name = "reset",          address = [2]},
    { name = "emergency_stop", address = [3]},
  ]
  coils = [
    { name = "motor1_run",     address = [0]},
    { name = "motor1_jog",     address = [1]},
    { name = "motor1_stop",    address = [2]},
  ]

  ## Analog Variables, Input Registers and Holding Registers
  ## measurement - the (optional) measurement name, defaults to "modbus"
  ## name        - the variable name
  ## byte_order  - the ordering of bytes
  ##  |---AB, ABCD   - Big Endian
  ##  |---BA, DCBA   - Little Endian
  ##  |---BADC       - Mid-Big Endian
  ##  |---CDAB       - Mid-Little Endian
  ## data_type  - INT16, UINT16, INT32, UINT32, INT64, UINT64,
  ##              FLOAT32-IEEE, FLOAT64-IEEE (the IEEE 754 binary representation)
  ##              FLOAT32, FIXED, UFIXED (fixed-point representation on input)
  ## scale      - the final numeric variable representation
  ## address    - variable address

  holding_registers = [
    { name = "power_factor", byte_order = "AB",   data_type = "FIXED", scale=0.01,  address = [8]},
    { name = "voltage",      byte_order = "AB",   data_type = "FIXED", scale=0.1,   address = [0]},
    { name = "energy",       byte_order = "ABCD", data_type = "FIXED", scale=0.001, address = [5,6]},
    { name = "current",      byte_order = "ABCD", data_type = "FIXED", scale=0.001, address = [1,2]},
    { name = "frequency",    byte_order = "AB",   data_type = "UFIXED", scale=0.1,  address = [7]},
    { name = "power",        byte_order = "ABCD", data_type = "UFIXED", scale=0.1,  address = [3,4]},
  ]
  input_registers = [
    { name = "tank_level",   byte_order = "AB",   data_type = "INT16",   scale=1.0,     address = [0]},
    { name = "tank_ph",      byte_order = "AB",   data_type = "INT16",   scale=1.0,     address = [1]},
    { name = "pump1_speed",  byte_order = "ABCD", data_type = "INT32",   scale=1.0,     address = [3,4]},
  ]

  ## Enable workarounds required by some devices to work correctly
  # [inputs.modbusx.workarounds]
    ## Pause between read requests sent to the device. This might be necessary for (slow) serial devices.
    # pause_between_requests = "0ms"
    ## Close the connection after every gather cycle. Usually the plugin closes the connection after a certain
    ## idle-timeout, however, if you query a device with limited simultaneous connectivity (e.g. serial devices)
    ## from multiple instances you might want to only stay connected during gather and disconnect afterwards.
    # close_connection_after_gather = false
 ## Enable write command
 # [inputs.modbusx.write_command]
	## redis url.
	# redis_url = "127.0.0.1:6379"
	## redis pass.
	# redis_pass = "123456"
	## redis channel.
	# redis_channel = "modbusx_write_command"

`

var measurement = "modbusx"
var lockReleaseTime = 50 * time.Millisecond

// SampleConfig returns a basic configuration for the plugin
func (m *ModbusX) SampleConfig() string {
	return sampleConfig
}

// Description returns a short description of what the plugin does
func (m *ModbusX) Description() string {
	return description
}

func (m *ModbusX) Init() error {
	//check device name
	if m.Name == "" {
		return fmt.Errorf("device name is empty")
	}

	if m.Retries < 0 {
		return fmt.Errorf("retries cannot be negative")
	}

	// Check and process the configuration
	if err := m.ConfigurationOriginal.Check(); err != nil {
		return fmt.Errorf("original configuraton invalid: %v", err)
	}

	r, err := m.ConfigurationOriginal.Process()
	if err != nil {
		return fmt.Errorf("cannot process original configuraton: %v", err)
	}
	m.requests = r

	// Setup client
	if err := m.initClient(); err != nil {
		return fmt.Errorf("initializing client failed: %v", err)
	}

	return nil
}

func (m *ModbusX) Start(acc telegraf.Accumulator) error {
	if m.NodeId != 0 {
		return nil
	}
	t, ok := acc.GetTags()["node_id"]
	if ok {
		nodeId, err := strconv.ParseInt(t, 10, 64)
		if err == nil {
			m.NodeId = int(nodeId)
		}
	}
	return nil
}

func (m *ModbusX) Stop() {

}

func (m *ModbusX) getGatherErr() error {
	for _, reqs := range m.ReqStatus {
		if reqs.Err != nil {
			fmt.Printf("node_id %d controller %s error: %s \n", m.NodeId, m.Controller, reqs.Err.Error())
			return reqs.Err
		}
	}
	return nil
}

// Gather implements the telegraf plugin interface method for data accumulation
func (m *ModbusX) Gather(acc telegraf.Accumulator) error {
	m.emptyStatus()
	m.Start(acc)
	m.ReqTimes = 0
	gatherStartTime := time.Now()
	var allBlockes int
	for _, rr := range m.requests {
		allBlockes += len(rr.coil)
		allBlockes += len(rr.discrete)
		allBlockes += len(rr.holding)
		allBlockes += len(rr.input)
	}
	if allBlockes < 2 {
		m.BlockIntervalTime = 80
	} else {
		m.BlockIntervalTime = int(acc.Getinterval()) / int(time.Millisecond) / 20 / allBlockes
		if m.BlockIntervalTime > 1000 {
			m.BlockIntervalTime = 950
		}
		if m.BlockIntervalTime < 100 {
			m.BlockIntervalTime = 100
		}
	}

	var connected = false

	timestamp := time.Now()

	var deviceOnlineFields = make(map[string]interface{})
	var deviceOnlineTags = make(map[string]string)
	deviceOnlineTags["SYS_TAG"] = "DTCT_SYS_OLSTATE"

	if !m.isConnected {
		for retry := 0; retry <= m.Retries; retry++ {
			err := m.connect()
			if err != nil {
				m.Log.Errorf("第 %d 次 连接失败 %s \n", retry, m.Controller)
				// return err
			} else {
				connected = true
				break
			}
		}

		if !connected {
			deviceOnlineFields["device_online"] = 1
			acc.AddGauge(measurement, deviceOnlineFields, deviceOnlineTags, timestamp)
			return errors.New(fmt.Sprintf("%s 连接失败", m.Controller))
		}

	} else {
		connected = true
	}
	var gatherFieldserr error
	for retry := 0; retry <= m.Retries; retry++ {
		timestamp = time.Now()
		gatherFieldserr = m.gatherFields()
		gatherFieldserr = m.getGatherErr()
		if gatherFieldserr != nil {
			if mberr, ok := gatherFieldserr.(*mb.Error); ok && mberr.ExceptionCode == mb.ExceptionCodeServerDeviceBusy && retry < m.Retries {
				m.Log.Infof("Device busy! Retrying %d more time(s)...", m.Retries-retry)
				time.Sleep(time.Duration(m.BlockIntervalTime*10) * time.Millisecond)
				continue
			}
			// Show the disconnect error this way to not shadow the initial error
			if discerr := m.disconnect(); discerr != nil {
				m.Log.Errorf("Disconnecting failed: %v", discerr)
			}

			//断线重连机制
			u, _ := url.Parse(m.Controller)
			if u.Scheme == "tcp" {
				//OpError, ok := gatherFieldserr.(*net.OpError)
				OpError := gatherFieldserr.Error()

				if strings.Contains(OpError, "write: broken pipe") || strings.Contains(OpError, "EOF") {
					m.handler.Close()
					m.isConnected = false
					fmt.Printf("断线重连中......\n")
				}

				if strings.Contains(OpError, "timeout") || strings.Contains(OpError, "EOF") {
					fmt.Printf("%s SlaveID %d 当前超时次数%d\n", m.Controller, m.SlaveID, getTryTimes(m.Controller))
					if getTryTimes(m.Controller) > 10 {
						m.handler.Close()
						m.isConnected = false
						fmt.Printf("断线重连中......\n")
						setTryTimes(m.Controller, 0)
						time.Sleep(5 * time.Second)
					}
					setTryTimes(m.Controller, getTryTimes(m.Controller)+1)
				}

				//if strings.Contains(OpError, "does not match request") {
				//	time.Sleep(3 * time.Second)
				//}

				fmt.Printf("errType --> %s\n", OpError)
			}

		} else {
			// Reading was successful, leave the retry loop
			break
		}

	}

	if gatherFieldserr != nil {
		deviceOnlineFields["device_online"] = 1
		acc.AddGauge(measurement, deviceOnlineFields, deviceOnlineTags, timestamp)
		m.addReport(gatherStartTime, acc)
		return gatherFieldserr
	}

	//成功采集后清空超时次数
	setTryTimes(m.Controller, 0)

	for slaveID, requests := range m.requests {
		tags := map[string]string{
			"name":     m.Name,
			"type":     cCoils,
			"slave_id": strconv.Itoa(int(slaveID)),
		}
		m.collectFields(acc, timestamp, tags, requests.coil)

		tags["type"] = cDiscreteInputs
		m.collectFields(acc, timestamp, tags, requests.discrete)

		tags["type"] = cHoldingRegisters
		m.collectFields(acc, timestamp, tags, requests.holding)

		tags["type"] = cInputRegisters
		m.collectFields(acc, timestamp, tags, requests.input)
	}

	// Disconnect after read if configured
	if m.Workarounds.CloseAfterGather {
		return m.disconnect()
	}

	deviceOnlineFields["device_online"] = 0
	acc.AddGauge(measurement, deviceOnlineFields, deviceOnlineTags, timestamp)

	m.addReport(gatherStartTime, acc)
	return nil
}

func getTryTimes(controller string) int {
	return tryTimes.Get(controller)
}

func setTryTimes(controller string, times int) bool {
	tryTimes.Set(controller, times)
	return true
}

func (m *ModbusX) initClient() error {
	u, err := url.Parse(m.Controller)
	if err != nil {
		return err
	}

	var timeoutTmp time.Duration
	if time.Duration(m.Workarounds.PollPause*5) > time.Duration(m.Timeout) {
		timeoutTmp = time.Duration(m.Timeout)
	} else {
		timeoutTmp = time.Duration(m.Workarounds.PollPause * 5)
	}

	if timeoutTmp > 1*time.Second {
		timeoutTmp = 1 * time.Second
	}

	if timeoutTmp < 500*time.Millisecond {
		timeoutTmp = 500 * time.Millisecond
	}

	switch u.Scheme {
	case "tcp":
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			return err
		}
		var handler_tmp mb.ClientHandler
		switch m.TransmissionMode {
		case "RTUoverTCP":

			var lock *sync.Mutex
			for _, RTUOverTCPClientHandler := range RTUOverTCPClientHandlerCache {
				if RTUOverTCPClientHandler.Controller == m.Controller {
					handler_tmp = RTUOverTCPClientHandler.handler
					lock = &RTUOverTCPClientHandler.CacheLock
					break
				}
			}

			if handler_tmp == nil {
				handler := mb.NewRTUOverTCPClientHandler(host + ":" + port)
				handler.Timeout = timeoutTmp
				if m.DebugConnection {
					handler.Logger = m
				}
				m.handler = handler

				// 存入緩存
				var RTUOverTCPClientHandlerTmp RTUOverTCPClientHandlerMap
				RTUOverTCPClientHandlerTmp.handler = handler
				RTUOverTCPClientHandlerTmp.Controller = m.Controller
				m.CacheLock = &RTUOverTCPClientHandlerTmp.CacheLock
				RTUOverTCPClientHandlerCache = append(RTUOverTCPClientHandlerCache, &RTUOverTCPClientHandlerTmp)
				fmt.Printf("RTUOverTCP 创建串口实例 %s \n", u.Path)
			} else {
				m.handler = handler_tmp
				m.CacheLock = lock
				fmt.Printf("RTUOverTCP 使用缓存句柄 %s \n", u.Path)
			}

		case "ASCIIoverTCP":

			var lock *sync.Mutex
			for _, ASCIIOverTCPClientHandler := range ASCIIOverTCPClientHandlerCache {
				if ASCIIOverTCPClientHandler.Controller == m.Controller {
					handler_tmp = ASCIIOverTCPClientHandler.handler
					lock = &ASCIIOverTCPClientHandler.CacheLock
					break
				}
			}

			if handler_tmp == nil {
				handler := mb.NewASCIIOverTCPClientHandler(host + ":" + port)
				handler.Timeout = timeoutTmp
				if m.DebugConnection {
					handler.Logger = m
				}
				m.handler = handler

				// 存入緩存
				var ASCIIOverTCPClientHandlerTmp ASCIIOverTCPClientHandlerMap
				ASCIIOverTCPClientHandlerTmp.handler = handler
				ASCIIOverTCPClientHandlerTmp.Controller = m.Controller
				m.CacheLock = &ASCIIOverTCPClientHandlerTmp.CacheLock
				ASCIIOverTCPClientHandlerCache = append(ASCIIOverTCPClientHandlerCache, &ASCIIOverTCPClientHandlerTmp)
				fmt.Printf("ASCIIoverTCP 创建串口实例 %s \n", u.Path)
			} else {
				m.handler = handler_tmp
				m.CacheLock = lock
				fmt.Printf("ASCIIoverTCP 使用缓存句柄 %s \n", u.Path)
			}

		default:
			handler := mb.NewTCPClientHandler(host + ":" + port)
			handler.LinkRecoveryTimeout = time.Duration(m.Timeout)
			handler.ProtocolRecoveryTimeout = time.Duration(m.Timeout)
			handler.Timeout = time.Duration(m.Timeout)
			if m.DebugConnection {
				handler.Logger = m
			}
			m.handler = handler
		}
	case "file":
		var handler_tmp mb.ClientHandler
		switch m.TransmissionMode {
		case "RTU":
			var lock *sync.Mutex
			for _, RTUClientHandler := range RTUClientHandlerCache {
				if RTUClientHandler.Controller == u.Path {
					handler_tmp = RTUClientHandler.handler
					lock = &RTUClientHandler.CacheLock
					break
				}
			}
			if handler_tmp == nil {
				handler := mb.NewRTUClientHandler(u.Path)
				handler.Timeout = timeoutTmp

				handler.BaudRate = m.BaudRate
				handler.DataBits = m.DataBits
				handler.Parity = m.Parity
				handler.StopBits = m.StopBits
				m.handler = handler

				// 存入緩存
				var RTUClientHandlerTmp RTUClientHandlerMap
				RTUClientHandlerTmp.handler = handler
				RTUClientHandlerTmp.Controller = u.Path
				m.CacheLock = &RTUClientHandlerTmp.CacheLock
				RTUClientHandlerCache = append(RTUClientHandlerCache, &RTUClientHandlerTmp)
				fmt.Printf("RTU 创建串口实例 %s \n", u.Path)
			} else {
				m.handler = handler_tmp
				m.CacheLock = lock
				fmt.Printf("RTU 使用缓存句柄 %s \n", u.Path)
			}
		case "ASCII":
			var lock *sync.Mutex
			for _, ASCIIClientHandler := range ASCIIClientHandlerCache {
				if ASCIIClientHandler.Controller == u.Path {
					lock = &ASCIIClientHandler.CacheLock
					handler_tmp = ASCIIClientHandler.handler
					break
				}
			}
			if handler_tmp == nil {
				handler := mb.NewASCIIClientHandler(u.Path)
				handler.Timeout = timeoutTmp
				handler.BaudRate = m.BaudRate
				handler.DataBits = m.DataBits
				handler.Parity = m.Parity
				handler.StopBits = m.StopBits
				m.handler = handler
				// 存入緩存
				var ASCIIClientHandlerTmp ASCIIClientHandlerMap
				ASCIIClientHandlerTmp.handler = handler
				ASCIIClientHandlerTmp.Controller = u.Path
				m.CacheLock = &ASCIIClientHandlerTmp.CacheLock
				ASCIIClientHandlerCache = append(ASCIIClientHandlerCache, &ASCIIClientHandlerTmp)
				fmt.Printf("ASCII 创建串口实例 %s \n", u.Path)
			} else {
				m.handler = handler_tmp
				m.CacheLock = lock
				fmt.Printf("ASCII 使用缓存句柄 %s \n", u.Path)
			}
		default:
			return fmt.Errorf("invalid protocol '%s' - '%s' ", u.Scheme, m.TransmissionMode)
		}
	default:
		return fmt.Errorf("invalid controller %q", m.Controller)
	}

	m.handler.SetSlave(m.SlaveID)
	m.client = mb.NewClient(m.handler)
	m.isConnected = false

	return nil
}

// Connect to a MODBUS Slave device via Modbus/[TCP|RTU|ASCII]
func (m *ModbusX) connect() error {
	err := m.handler.Connect()
	m.isConnected = err == nil
	return err
}

func (m *ModbusX) disconnect() error {
	var err error
	if m.TransmissionMode == "ASCII" || m.TransmissionMode == "RTU" {
		err = m.handler.Close()
	}
	m.isConnected = false
	return err
}

func (m *ModbusX) gatherFields() error {
	for _, requests := range m.requests {
		if err := m.gatherRequestsCoil(requests.coil); err != nil {
			return err
		}
		if err := m.gatherRequestsDiscrete(requests.discrete); err != nil {
			return err
		}
		if err := m.gatherRequestsHolding(requests.holding); err != nil {
			return err
		}
		if err := m.gatherRequestsInput(requests.input); err != nil {
			return err
		}
	}

	return nil
}

func (m *ModbusX) gatherRequestsCoil(requests []request) error {
	//u, _ := url.Parse(m.Controller)
	for _, req := range requests {
		var bytes []byte
		m.ReqTimes++
		m.Log.Debugf("trying to read coil@%v[%v]...", req.address, req.length)
		if m.TransmissionMode == "ASCII" || m.TransmissionMode == "RTU" || m.TransmissionMode == "RTUoverTCP" || m.TransmissionMode == "ASCIIoverTCP" {
			m.CacheLock.Lock()
		}
		m.handler.SetSlave(m.SlaveID)
		m.client = mb.NewClient(m.handler)
		t := time.Now()
		var err error
		bytes, err = m.client.ReadCoils(req.address, req.length)
		m.addStatus(1, req, err)
		elapsed := time.Since(t)
		m.Log.Debugf("modbusx 串口号 %s SlaveID %d 发送请求 gatherRequestsCoil 地址 %d 长度 %d 采集消耗: %d ms\n", m.Controller, m.SlaveID, req.address, req.length, elapsed.Milliseconds())
		time.Sleep(time.Duration(m.BlockIntervalTime) * time.Millisecond)
		if m.TransmissionMode == "ASCII" || m.TransmissionMode == "RTU" || m.TransmissionMode == "RTUoverTCP" || m.TransmissionMode == "ASCIIoverTCP" {
			m.CacheLock.Unlock()
		}
		if err != nil {
			continue
		}
		nextRequest := time.Now().Add(time.Duration(m.Workarounds.PollPause))
		m.Log.Debugf("got coil@%v[%v]: %v", req.address, req.length, bytes)

		// Bit value handling
		for i, field := range req.fields {
			offset := field.address - req.address
			idx := offset / 8
			bit := offset % 8

			req.fields[i].value = uint16((bytes[idx] >> bit) & 0x01)
			m.Log.Debugf("  field %s with bit %d @ byte %d: %v --> %v", field.name, bit, idx, (bytes[idx]>>bit)&0x01, req.fields[i].value)
		}

		// Some (serial) devices require a pause between requests...
		time.Sleep(time.Until(nextRequest))
	}
	return nil
}

func (m *ModbusX) gatherRequestsDiscrete(requests []request) error {
	//u, _ := url.Parse(m.Controller)
	for _, req := range requests {
		var bytes []byte
		m.ReqTimes++
		m.Log.Debugf("trying to read discrete@%v[%v]...", req.address, req.length)
		m.Log.Debugf("trying to read coil@%v[%v]...", req.address, req.length)
		if m.TransmissionMode == "ASCII" || m.TransmissionMode == "RTU" || m.TransmissionMode == "RTUoverTCP" || m.TransmissionMode == "ASCIIoverTCP" {
			m.CacheLock.Lock()
		}
		m.handler.SetSlave(m.SlaveID)
		m.client = mb.NewClient(m.handler)
		t := time.Now()
		var err error
		bytes, err = m.client.ReadDiscreteInputs(req.address, req.length)
		m.addStatus(2, req, err)
		elapsed := time.Since(t)
		m.Log.Debugf("modbusx 串口号 %s SlaveID %d 发送请求 gatherRequestsDiscrete 地址 %d 长度 %d 采集消耗: %d ms\n", m.Controller, m.SlaveID, req.address, req.length, elapsed.Milliseconds())
		time.Sleep(time.Duration(m.BlockIntervalTime) * time.Millisecond)
		if m.TransmissionMode == "ASCII" || m.TransmissionMode == "RTU" || m.TransmissionMode == "RTUoverTCP" || m.TransmissionMode == "ASCIIoverTCP" {
			m.CacheLock.Unlock()
		}
		if err != nil {
			continue
		}
		nextRequest := time.Now().Add(time.Duration(m.Workarounds.PollPause))
		m.Log.Debugf("got discrete@%v[%v]: %v", req.address, req.length, bytes)

		// Bit value handling
		for i, field := range req.fields {
			offset := field.address - req.address
			idx := offset / 8
			bit := offset % 8

			req.fields[i].value = uint16((bytes[idx] >> bit) & 0x01)
			m.Log.Debugf("  field %s with bit %d @ byte %d: %v --> %v", field.name, bit, idx, (bytes[idx]>>bit)&0x01, req.fields[i].value)
		}

		// Some (serial) devices require a pause between requests...
		time.Sleep(time.Until(nextRequest))
	}
	return nil
}

func (m *ModbusX) gatherRequestsHolding(requests []request) error {
	//u, _ := url.Parse(m.Controller)
	for _, req := range requests {
		var bytes []byte
		m.ReqTimes++
		m.Log.Debugf("trying to read holding@%v[%v]...", req.address, req.length)
		m.Log.Debugf("trying to read coil@%v[%v]...", req.address, req.length)
		if m.TransmissionMode == "ASCII" || m.TransmissionMode == "RTU" || m.TransmissionMode == "RTUoverTCP" || m.TransmissionMode == "ASCIIoverTCP" {
			m.CacheLock.Lock()
		}
		m.handler.SetSlave(m.SlaveID)
		m.client = mb.NewClient(m.handler)
		t := time.Now()
		var err error
		bytes, err = m.client.ReadHoldingRegisters(req.address, req.length)
		m.addStatus(3, req, err)
		elapsed := time.Since(t)
		m.Log.Debugf("modbusx 串口号 %s SlaveID %d 发送请求 gatherRequestsHolding 地址 %d 长度 %d 采集消耗: %d ms\n", m.Controller, m.SlaveID, req.address, req.length, elapsed.Milliseconds())
		time.Sleep(time.Duration(m.BlockIntervalTime) * time.Millisecond)
		if m.TransmissionMode == "ASCII" || m.TransmissionMode == "RTU" || m.TransmissionMode == "RTUoverTCP" || m.TransmissionMode == "ASCIIoverTCP" {
			m.CacheLock.Unlock()
		}
		if err != nil {
			continue
		}
		nextRequest := time.Now().Add(time.Duration(m.Workarounds.PollPause))
		m.Log.Debugf("got holding@%v[%v]: %v", req.address, req.length, bytes)

		// Non-bit value handling
		for i, field := range req.fields {
			// Determine the offset of the field values in the read array
			offset := 2 * (field.address - req.address) // registers are 16bit = 2 byte
			length := 2 * field.length                  // field length is in registers a 16bit

			// Convert the actual value
			req.fields[i].value = field.converter(bytes[offset : offset+length])
			m.Log.Debugf("  field %s with offset %d with len %d: %v --> %v", field.name, offset, length, bytes[offset:offset+length], req.fields[i].value)
		}

		// Some (serial) devices require a pause between requests...
		time.Sleep(time.Until(nextRequest))
	}
	return nil
}

func (m *ModbusX) gatherRequestsInput(requests []request) error {
	//u, _ := url.Parse(m.Controller)
	for _, req := range requests {
		var bytes []byte
		m.ReqTimes++
		m.Log.Debugf("trying to read input@%v[%v]...", req.address, req.length)
		m.Log.Debugf("trying to read coil@%v[%v]...", req.address, req.length)
		if m.TransmissionMode == "ASCII" || m.TransmissionMode == "RTU" || m.TransmissionMode == "RTUoverTCP" || m.TransmissionMode == "ASCIIoverTCP" {
			m.CacheLock.Lock()
		}
		m.handler.SetSlave(m.SlaveID)
		m.client = mb.NewClient(m.handler)
		t := time.Now()
		var err error
		bytes, err = m.client.ReadInputRegisters(req.address, req.length)
		m.addStatus(2, req, err)
		elapsed := time.Since(t)
		m.Log.Debugf("modbusx 串口号 %s SlaveID %d 发送请求 gatherRequestsInput 地址 %d 长度 %d 采集消耗: %d ms\n", m.Controller, m.SlaveID, req.address, req.length, elapsed.Milliseconds())
		time.Sleep(time.Duration(m.BlockIntervalTime) * time.Millisecond)
		if m.TransmissionMode == "ASCII" || m.TransmissionMode == "RTU" || m.TransmissionMode == "RTUoverTCP" || m.TransmissionMode == "ASCIIoverTCP" {
			m.CacheLock.Unlock()
		}
		if err != nil {
			continue
		}
		nextRequest := time.Now().Add(time.Duration(m.Workarounds.PollPause))
		m.Log.Debugf("got input@%v[%v]: %v", req.address, req.length, bytes)

		// Non-bit value handling
		for i, field := range req.fields {
			// Determine the offset of the field values in the read array
			offset := 2 * (field.address - req.address) // registers are 16bit = 2 byte
			length := 2 * field.length                  // field length is in registers a 16bit

			// Convert the actual value
			req.fields[i].value = field.converter(bytes[offset : offset+length])
			m.Log.Debugf("  field %s with offset %d with len %d: %v --> %v", field.name, offset, length, bytes[offset:offset+length], req.fields[i].value)
		}

		// Some (serial) devices require a pause between requests...
		time.Sleep(time.Until(nextRequest))
	}
	return nil
}

func (m *ModbusX) collectFields(acc telegraf.Accumulator, timestamp time.Time, tags map[string]string, requests []request) {
	grouper := metric.NewSeriesGrouper()
	for _, request := range requests {
		for _, field := range request.fields {
			// In case no measurement was specified we use "modbus" as default
			measurement := "modbusx"
			if field.measurement != "" {
				measurement = field.measurement
			}

			// Group the data by series
			if err := grouper.Add(measurement, tags, timestamp, field.name, field.value); err != nil {
				acc.AddError(fmt.Errorf("cannot add field %q for measurement %q: %v", field.name, measurement, err))
				continue
			}
		}
	}

	// Add the metrics grouped by series to the accumulator
	for _, x := range grouper.Metrics() {
		acc.AddMetric(x)
	}
}

// Implement the logger interface of the modbus client
func (m *ModbusX) Printf(format string, v ...interface{}) {
	m.Log.Debugf(format, v...)
}

// Add this plugin to telegraf
func init() {
	inputs.Add("modbusx", func() telegraf.Input {
		return &ModbusX{Timeout: config.Duration(1 * time.Second),
			Retries: 3, RetriesWaitTime: config.Duration(100 * time.Millisecond),
			Workarounds:  ModbusWorkarounds{PollPause: config.Duration(10 * time.Millisecond)},
			WriteCommand: ModbusWriteCommand{RedisPass: "dtct123", RedisUrl: "127.0.0.1:6379", RedisChannel: "modbusx_write_command"},
		}
	})
}
