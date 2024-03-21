package models

import (
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/selfstat"
)

var (
	GlobalMetricsGathered = selfstat.Register("agent", "metrics_gathered", map[string]string{})
	GlobalGatherErrors    = selfstat.Register("agent", "gather_errors", map[string]string{})
	GlobalGatherTimeouts  = selfstat.Register("agent", "gather_timeouts", map[string]string{})
)

type RunningInput struct {
	Input  telegraf.Input
	Config *InputConfig

	log         telegraf.Logger
	defaultTags map[string]string

	MetricsGathered selfstat.Stat
	GatherTime      selfstat.Stat
	GatherTimeouts  selfstat.Stat
}

func NewRunningInput(input telegraf.Input, config *InputConfig) *RunningInput {
	tags := map[string]string{"input": config.Name}
	if config.Alias != "" {
		tags["alias"] = config.Alias
	}

	inputErrorsRegister := selfstat.Register("gather", "errors", tags)
	logger := NewLogger("inputs", config.Name, config.Alias)
	logger.OnErr(func() {
		inputErrorsRegister.Incr(1)
		GlobalGatherErrors.Incr(1)
	})
	SetLoggerOnPlugin(input, logger)

	return &RunningInput{
		Input:  input,
		Config: config,
		MetricsGathered: selfstat.Register(
			"gather",
			"metrics_gathered",
			tags,
		),
		GatherTime: selfstat.RegisterTiming(
			"gather",
			"gather_time_ns",
			tags,
		),
		GatherTimeouts: selfstat.Register(
			"gather",
			"gather_timeouts",
			tags,
		),
		log: logger,
	}
}

// InputConfig is the common config for all inputs.
type InputConfig struct {
	Name             string
	Alias            string
	ID               string
	DynamicInterval  bool // if true, new job will start after the end of last job immediately！！！
	Interval         time.Duration
	CollectionJitter time.Duration
	CollectionOffset time.Duration
	Precision        time.Duration

	NameOverride            string
	MeasurementPrefix       string
	MeasurementSuffix       string
	Tags                    map[string]string
	Filter                  Filter
	AlwaysIncludeLocalTags  bool
	AlwaysIncludeGlobalTags bool
}

func (r *RunningInput) metricFiltered(metric telegraf.Metric) {
	metric.Drop()
}

func (r *RunningInput) LogName() string {
	return logName("inputs", r.Config.Name, r.Config.Alias)
}

func (r *RunningInput) Init() error {
	if p, ok := r.Input.(telegraf.Initializer); ok {
		err := p.Init()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RunningInput) ID() string {
	if p, ok := r.Input.(telegraf.PluginWithID); ok {
		return p.ID()
	}
	return r.Config.ID
}

func (r *RunningInput) MakeMetric(metric telegraf.Metric) telegraf.Metric {
	ok, err := r.Config.Filter.Select(metric)
	if err != nil {
		r.log.Errorf("filtering failed: %v", err)
	} else if !ok {
		r.metricFiltered(metric)
		return nil
	}

	makemetric(
		metric,
		r.Config.NameOverride,
		r.Config.MeasurementPrefix,
		r.Config.MeasurementSuffix,
		r.Config.Tags,
		r.defaultTags)

	r.Config.Filter.Modify(metric)
	if len(metric.FieldList()) == 0 {
		r.metricFiltered(metric)
		return nil
	}

	if r.Config.AlwaysIncludeLocalTags || r.Config.AlwaysIncludeGlobalTags {
		var local, global map[string]string
		if r.Config.AlwaysIncludeLocalTags {
			local = r.Config.Tags
		}
		if r.Config.AlwaysIncludeGlobalTags {
			global = r.defaultTags
		}
		makemetric(metric, "", "", "", local, global)
	}

	r.MetricsGathered.Incr(1)
	GlobalMetricsGathered.Incr(1)
	return metric
}

func (r *RunningInput) Gather(acc telegraf.Accumulator) error {
	start := time.Now()
	err := r.Input.Gather(acc)
	elapsed := time.Since(start)
	r.GatherTime.Incr(elapsed.Nanoseconds())
	return err
}

func (r *RunningInput) SetDefaultTags(tags map[string]string) {
	r.defaultTags = tags
}

func (r *RunningInput) Log() telegraf.Logger {
	return r.log
}

func (r *RunningInput) IncrGatherTimeouts() {
	GlobalGatherTimeouts.Incr(1)
	r.GatherTimeouts.Incr(1)
}
