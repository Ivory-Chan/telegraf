package modbusx

import (
	mb "github.com/grid-x/modbus"
	"github.com/influxdata/telegraf"
	"strings"
	"time"
)

const preFlag = "MEd9x"

type reqStatus struct {
	FuncCode int
	Request  request
	Err      error
}

func (m *ModbusX) addStatus(funcCode int, r request, err error) {
	m.ReqLock.Lock()
	defer m.ReqLock.Unlock()
	var index int
	var s reqStatus
	var isAdd bool
	for index, s = range m.ReqStatus {
		if s.Request.address == r.address && s.Request.length == r.length && funcCode == s.FuncCode {
			isAdd = true
			if err == nil {
				m.ReqStatus[index].Err = err
				return
			}
		}
	}

	if !isAdd {
		var req reqStatus
		req.Request = r
		req.FuncCode = funcCode
		req.Err = err
		m.ReqStatus = append(m.ReqStatus, req)
	}
}

func (m *ModbusX) emptyStatus() {
	m.ReqLock.Lock()
	defer m.ReqLock.Unlock()
	m.ReqStatus = nil
}

func (m *ModbusX) addReport(startTime time.Time, acc telegraf.Accumulator) {
	totalPacket, crcErrPacket, timeoutPacket, errPacket, otherErr := m.reqCount()
	tag := make(map[string]string)
	fileds := make(map[string]interface{})
	tag["m"] = "packet_total"
	fileds[preFlag+"dev_comm"] = totalPacket
	acc.AddGauge(measurement, fileds, tag, startTime)

	fileds[preFlag+"dev_comm"] = errPacket
	tag["m"] = "packet_err"
	acc.AddGauge(measurement, fileds, tag, startTime)

	fileds[preFlag+"dev_comm"] = crcErrPacket
	tag["m"] = "packet_crc_err"
	acc.AddGauge(measurement, fileds, tag, startTime)

	fileds[preFlag+"dev_comm"] = timeoutPacket
	tag["m"] = "packet_timeout"
	acc.AddGauge(measurement, fileds, tag, startTime)

	fileds[preFlag+"dev_comm"] = otherErr
	tag["m"] = "packet_other_err"
	acc.AddGauge(measurement, fileds, tag, startTime)

	fileds[preFlag+"dev_comm"] = int(time.Since(startTime) / time.Millisecond)
	tag["m"] = "gather_time"
	acc.AddGauge(measurement, fileds, tag, startTime)
	var errPoint float64
	if totalPacket > 0 {
		errPoint = float64(errPacket+crcErrPacket+timeoutPacket) / float64(totalPacket) * 1000
	}

	if errPoint > 10 {
		fileds[preFlag+"dev_comm"] = 1
		tag["m"] = "comm_err"
		acc.AddGauge(measurement, fileds, tag, startTime)
	}
	if errPoint <= 10 && errPoint > 1 {
		fileds[preFlag+"dev_comm"] = 1
		tag["m"] = "comm_shake"
		acc.AddGauge(measurement, fileds, tag, startTime)
	}
	if errPoint <= 1 {
		fileds[preFlag+"dev_comm"] = 1
		tag["m"] = "comm_ok"
		acc.AddGauge(measurement, fileds, tag, startTime)
	}
}

func (m *ModbusX) reqCount() (totalPacket, crcErrPacket, timeoutPacket, errPacket, otherErr int) {
	m.ReqLock.Lock()
	m.ReqLock.Unlock()
	for _, r := range m.ReqStatus {
		totalPacket += 1
		addFlag := false
		if r.Err != nil {
			if strings.Contains(r.Err.Error(), "crc") {
				addFlag = true
				crcErrPacket += 1
			}
			if strings.Contains(r.Err.Error(), "timeout") {
				addFlag = true
				timeoutPacket += 1
			}
			if _, ok := r.Err.(*mb.Error); ok {
				if !addFlag {
					errPacket += 1
					addFlag = true
				}
			}
			// 其他原因统计到错误协议
			if !addFlag {
				otherErr += 1
			}
		}
	}
	return
}
