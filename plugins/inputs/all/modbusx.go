//go:build !custom || inputs || inputs.modbusx

package all

import _ "github.com/influxdata/telegraf/plugins/inputs/modbusx" // register plugin
