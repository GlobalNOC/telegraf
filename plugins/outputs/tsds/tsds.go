package tsds

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/influxdata/telegraf/grnoc/counter"
	"github.com/influxdata/telegraf/grnoc/wsc"
	//"github.grnoc.iu.edu/jdratlif/GRNOC-Counter-go/counter"
	//"github.grnoc.iu.edu/jdratlif/GRNOC-WebService-Client-go/wsc"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
)

var rc = counter.NewCounter()

// TSDS is an output plugin that sends telegraf metrics to a TSDS server
type TSDS struct {
	Type     string   `toml:"measurement"`
	Hostname string   `toml:"hostname"`
	Username string   `toml:"username"`
	Password string   `toml:"password"`
	Interval int      `toml:"interval"`
	Sensors  []string `toml:"sensors"`
	Metadata []string `toml:"metadata"`
	Rates    []string `toml:"rates"`

	Pathmap map[string]string
	client  *wsc.WebServiceClient
	Log     telegraf.Logger `toml:"-"`
}

type Data struct {
	Type      string                 `json:"type"`
	Timestamp int64                  `json:"time"`
	Interval  int                    `json:"interval"`
	Metadata  map[string]interface{} `json:"meta"`
	Values    map[string]interface{} `json:"values"`
}

// Connect is called to open a connection to the TSDS server
// There are no long running connections to a TSDS server
// This method simply creates the WebServiceClient that will be used
// to send data to the TSDS server
// http.Client handles the keep-alive and idle timeouts automatically
func (server *TSDS) Connect() error {

	// Setup the TSDS WSC API handler
	server.client = &wsc.WebServiceClient{
		URL:      fmt.Sprintf("https://%s/tsds-basic/services/push.cgi", server.Hostname),
		Username: server.Username,
		Password: server.Password,
	}

	return nil
}

// Close is called to terminate the connection to the TSDS server
// There are no long running connections to the TSDS server to close
// We just let http.Client timeout the idle keep-alive connections
func (server *TSDS) Close() error {
	return nil
}

// Description returns a short description of this output plugin
func (server *TSDS) Description() string {
	return "Send telegraf metric(s) to TSDS server"
}

// SampleConfig returns a config sample for this plugin
func (server *TSDS) SampleConfig() string {
	return `
		## TSDS measurement type
		measurement = "interface"

		## TSDS Server Hostname/IP
		hostname = "io3.bldc.grnoc.iu.edu"

		## TSDS Username
		username = "tsds"

		## TSDS Password
		password = "BEWARE WORLD READABLE PASSWORDS"

		## interval between metrics
		interval = 60

		## The JTI Sensors from the input
		sensors = ["alias1 /resource/path1", "alias2 /resource/path2"]
	`
}

// send() takes Data and posts it to TSDS query.cgi
func (server *TSDS) Send(outputs []string) error {

	server.Log.Infof("Sending %v measurements to TSDS", len(outputs))

	// Create a stringified array of the output objects
	url_data := "[" + strings.Join(outputs, ",") + "]"

	// POST the data to query.cgi
	_, _, err := server.client.Post("add_data", map[string]string{"data": url_data})

	if err != nil {
		server.Log.Errorf("Could not send output to TSDS: %v\n%v", url_data, err)
	}
	return err
}

// Write() takes raw Metrics, parses them to Data structs, and sends them to the TSDS server as JSON
// This function runs every time the telegraf interval passes and metrics are present
func (server *TSDS) Write(metrics []telegraf.Metric) error {

	server.Log.Debugf("Writing metrics to TSDS")

	// Initialize the Pathmap for resource paths and TSDS measurements
	server.setPathmap()

	// Parse the raw Metrics into Data structs
	data := server.parseMetrics(metrics)

	// Store JSON strings for each Data struct in here for POSTing
	output := []string{}

	// Send each Data struct to TSDS as JSON
	for _, entry := range data {

		/* Set the node once before writing if it's incorrect
		if name, found := entry.Metadata["node"]; found && name != node {
			entry.Metadata["node"] = node
		}*/

		// Convert the Data struct to JSON
		bytes, err := json.Marshal(entry)
		if err != nil {
			server.Log.Errorf("Could not convert entry to JSON: %v\n%v", entry, err)
		}

		// Add the JSON string to outputs
		output = append(output, string(bytes))

		// TSDS expects batches per POST to push.cgi, this is TRADITIONALLY 50
		if len(output) >= 50 {

			server.Log.Debugf("Sending this to TSDS:\n%v", output)
			// Send the output
			server.Send(output)

			// Reset the output array
			output = []string{}
		}
	}

	//server.Log.Debugf("Output being sent:\n%v", output)
	// Send output when all data has been processed if < batch size
	if len(output) > 0 {
		server.Send(output)
	}

	return nil
}

//setPathmap() initializes a map for resource paths to their TSDS measurement names
func (server *TSDS) setPathmap() {
	// Check for Sensors and whether Pathmap has been initialized
	if len(server.Sensors) > 0 && server.Pathmap == nil {

		// Initialize the Pathmap
		server.Pathmap = make(map[string]string)

		server.Log.Debugf("Initializing TSDS.Pathmap using sources from config")

		// Map sensor aliases to their resource path given in config
		var alias, rpath string
		for _, sensor := range server.Sensors {
			split := strings.Split(sensor, " ")
			if len(split) > 1 {
				alias = split[0]
				rpath = split[1]
			} else {
				server.Log.Errorf("Malformed sensor string received: %v", sensor)
			}

			// Add the alias and resource path to the Pathmap
			server.Pathmap[rpath] = alias
		}
		server.Log.Debugf("Finished TSDS.Pathmap\n%v", server.Pathmap)
	}
}

// parseMetrics() reformats a telegraf.Metric into TSDS input
func (server *TSDS) parseMetrics(metrics []telegraf.Metric) (results []Data) {

	server.Log.Debugf("Received %v \"%v\" metrics for processing", len(metrics), server.Type)

	// Initialize a mapping for nodes to their data entries
	results = []Data{}

	good := 0
	// Check every metric received
	for _, metric := range metrics {

		// Tag and Field data from the Metric interface
		tags := metric.Tags()
		flds := metric.Fields()

		// Initialize a Data struct for the metric
		data := Data{
			Type:      server.Type,
			Timestamp: metric.Time().Unix(),
			Interval:  server.Interval,
			Metadata:  make(map[string]interface{}),
			Values:    make(map[string]interface{}),
		}

		// Get and then set the node name for the metadata
		node := tags["device"]
		data.Metadata["node"] = string(node)

		//server.Log.Debugf("Processing \"%v\" for %v", metric.Name(), node)

		// Parse the metadata for the Metric
		for k, v := range tags {
			// Assign any non-node metadata if found in the Pathmap
			if _, found := server.Pathmap[k]; found && server.Pathmap[k] != "node" {
				data.Metadata[server.Pathmap[k]] = string(v)
				//server.Log.Debugf("Found metadata for %v: %v", server.Pathmap[k], v)
			}
		}

		// Parse the value data for the Metric and process rates
		for k, v := range flds {

			// Assign a value if found in the Pathmap
			if _, found := server.Pathmap[k]; found {
				data.Values[server.Pathmap[k]] = v
			}
		}

		//server.Log.Debugf("Data has [%v/%v] requested measurements", len(data.Metadata)+len(data.Values), len(server.Pathmap)+1)
		if len(data.Metadata)+len(data.Values) >= len(server.Pathmap)+1 {
			good = good + 1

			// Process any data values that are rates
			if len(server.Rates) > 0 {
				server.ProcessRates(data)
			}

			// Add the good data to results
			results = append(results, data)
		}
	}
	server.Log.Debugf("%v of %v metrics produced good data", good, len(metrics))
	server.Log.Debugf("Finished processing all metrics!")
	return
}

func (server *TSDS) ProcessRates(data Data) {

	var ratekey string

	// Create a key for the rate calculator using all the metadata values
	if node, ok := data.Metadata["node"].(string); ok {
		ratekey = data.Type + node
		for _, metakey := range server.Metadata {
			if key, ok := data.Metadata[metakey].(string); ok {
				ratekey = ratekey + key
			}
		}
	}

	server.Log.Debugf("Calculating rate for ratekey: %v", ratekey)

	// Calculate the rate for any values listed as rates
	for _, measurement := range server.Rates {

		raw := data.Values[measurement]

		// TODO: rc.GetRate should accept an interface value and use typeswitch instead of forcing
		// excessive and unreliable type checking any time it's used
		if value, isNum := raw.(int64); !isNum {
			server.Log.Errorf("ProcessRates() given a non-number value: %v", value)
			continue

		} else {

			if _, found := rc.Values[ratekey]; !found {
				rc.Values[ratekey] = counter.NewMeasurement(
					60,
					data.Timestamp,
					counter.DefaultMinValue,
					counter.DefaultMaxValue,
					float64(value),
				)
				data.Values[measurement] = nil
				continue
			}

			rate, err := rc.GetRate(ratekey, data.Timestamp, float64(value))

			if err != nil {
				data.Values[measurement] = nil
				server.Log.Errorf("Could not calculate rate for %v!\n%v", measurement, err)
				continue
			}

			data.Values[measurement] = rate
		}
	}
}

func init() {
	// add this plugin to the available output plugins
	outputs.Add("tsds", func() telegraf.Output {
		return &TSDS{}
	})
}
